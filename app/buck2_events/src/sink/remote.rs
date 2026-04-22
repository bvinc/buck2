/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! A Sink for forwarding events directly to Remote service.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use fbinit::FacebookInit;

/// HTTP header with key and value (supports env var substitution via $VAR syntax).
#[derive(Clone, Debug)]
pub struct HttpHeader {
    pub key: String,
    pub value: String,
}

/// Configuration for connecting to a Bazel Build Event Service (BES) endpoint.
#[derive(Clone, Debug)]
pub struct BesConfig {
    /// BES gRPC endpoint address (e.g. "https://host:port").
    pub address: String,
    /// Whether to use TLS.
    pub tls: bool,
    /// HTTP headers to inject into requests.
    pub http_headers: Vec<HttpHeader>,
    /// Project ID sent with BES requests.
    pub project_id: String,
    /// URL prefix for build result links (trace ID is appended).
    pub result_url: Option<String>,
}

#[cfg(fbcode_build)]
mod fbcode {
    pub use scribe_client::ScribeConfig;

    pub use crate::sink::scribe::RemoteEventSink;
    pub(crate) use crate::sink::scribe::scribe_category;
}

#[cfg(not(fbcode_build))]
mod fbcode {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use anyhow::Context;

    use async_stream::stream;
    use async_trait::async_trait;

    use bazel_event_publisher_proto::build_event_stream;
    use bazel_event_publisher_proto::build_event_stream::build_event_id;
    use bazel_event_publisher_proto::build_event_stream::BuildEventId;
    use bazel_event_publisher_proto::google::devtools::build::v1;
    use buck2_data;
    use buck2_data::BuildCommandStart;
    use buck2_util::future::try_join_all;
    use dupe::Dupe;
    use futures::stream;
    use once_cell::sync::Lazy;

    use futures::Stream;
    use futures::StreamExt;
    use regex::Regex;
    use tonic::metadata;
    use tonic::metadata::MetadataKey;
    use tonic::metadata::MetadataValue;
    use tonic::service::interceptor::InterceptedService;
    use tonic::service::Interceptor;
    use tonic::transport::Channel;
    use tonic::transport::channel::ClientTlsConfig;
    use tonic::Request;

    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::UnboundedSender;

    use tokio_stream::wrappers::UnboundedReceiverStream;

    use bazel_event_publisher_proto::google::devtools::build::v1::OrderedBuildEvent;
    use bazel_event_publisher_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
    use bazel_event_publisher_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
    use bazel_event_publisher_proto::google::devtools::build::v1::PublishLifecycleEventRequest;
    use bazel_event_publisher_proto::google::devtools::build::v1::StreamId;

    use prost;
    use prost::Message;
    use prost_types;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    use super::BesConfig;
    use super::HttpHeader;

    pub struct RemoteEventSink {
        _handler: JoinHandle<()>,
        send: UnboundedSender<Vec<BuckEvent>>,
    }

    /// Replace occurrences of $FOO in a string with the value of the env var $FOO.
    fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
        static ENV_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

        let mut out = String::with_capacity(s.len());
        let mut last_idx: usize = 0;

        for mat in ENV_REGEX.find_iter(s) {
            out.push_str(&s[last_idx..mat.start()]);
            let var = &mat.as_str()[1..];
            let val = std::env::var(var)
                .with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
            out.push_str(&val);
            last_idx = mat.end();
        }

        if last_idx < s.len() {
            out.push_str(&s[last_idx..s.len()]);
        }

        Ok(out)
    }

    #[derive(Clone, Dupe)]
    struct InjectHeadersInterceptor {
        headers: Arc<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>>,
    }

    impl InjectHeadersInterceptor {
        pub fn new(headers: &[HttpHeader]) -> anyhow::Result<Self> {
            let headers = headers
                .iter()
                .map(|h| {
                    let key = substitute_env_vars(&h.key)?;
                    let value = substitute_env_vars(&h.value)?;

                    let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                        .with_context(|| format!("Invalid key in header: `{}: {}`", key, value))?;

                    let value = MetadataValue::try_from(&value)
                        .with_context(|| format!("Invalid value in header: `{}: {}`", key, value))?;

                    anyhow::Ok((key, value))
                })
                .collect::<Result<_, _>>()
                .context("Error converting headers")?;

            Ok(Self {
                headers: Arc::new(headers),
            })
        }
    }

    impl Interceptor for InjectHeadersInterceptor {
        fn call(
            &mut self,
            mut request: tonic::Request<()>,
        ) -> Result<tonic::Request<()>, tonic::Status> {
            for (k, v) in self.headers.iter() {
                request.metadata_mut().insert(k.clone(), v.clone());
            }
            Ok(request)
        }
    }

    type GrpcService = InterceptedService<Channel, InjectHeadersInterceptor>;

    async fn connect_build_event_server(
        config: &BesConfig,
    ) -> anyhow::Result<PublishBuildEventClient<GrpcService>> {
        let uri = config.address.parse()?;
        let mut channel = Channel::builder(uri);

        if config.tls {
            let tls_config = ClientTlsConfig::new().with_enabled_roots();
            // TODO: support tls_ca_certs and tls_client_cert
            channel = channel.tls_config(tls_config)?;
        }

        let endpoint = channel
            .connect()
            .await
            .context("connecting to BES gRPC server")?;

        let interceptor = InjectHeadersInterceptor::new(&config.http_headers)?;
        let client = PublishBuildEventClient::with_interceptor(endpoint, interceptor);
        Ok(client)
    }

    /// Tagged event indicating which BES RPC transport to use.
    enum BesTransportEvent {
        /// Sent via PublishLifecycleEvent unary RPC.
        Lifecycle(v1::BuildEvent),
        /// Sent via PublishBuildToolEventStream streaming RPC.
        Stream(v1::BuildEvent),
    }

    /// Convert a Buck2 label to a valid Bazel label string.
    ///
    /// HACK: Buck2 labels use `cell//pkg:target` (e.g. `root//foo:bar`) which is
    /// not a valid Bazel label. BES servers like EngFlow parse these as Bazel
    /// labels and reject the `cell//` prefix. As a workaround we prepend `@` to
    /// produce `@cell//pkg:target`, which is syntactically valid Bazel (external
    /// repo label) but semantically incorrect — Buck2 cells are not Bazel repos.
    ///
    /// This needs a proper solution in the future, e.g. a BES-server-side label
    /// format that understands Buck2 cells, or a configurable label mapping.
    fn bazel_label(package: &str, name: &str) -> String {
        if package.starts_with("//") || package.starts_with('@') {
            format!("{}:{}", package, name)
        } else {
            format!("@{}:{}", package, name)
        }
    }

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(events: S) -> impl Stream<Item = BesTransportEvent> {
        let mut target_actions: HashMap<(String, String), Vec<(BuildEventId, bool)>> = HashMap::new();
        // Track configured targets for the BEP event graph: (label, config_full_name, rule_type)
        let mut configured_targets: Vec<(String, String, String)> = Vec::new();
        // Track which configurations have been emitted to avoid duplicates
        let mut emitted_configurations: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Track parsed target patterns from ParsedTargetPatterns instant event
        let mut parsed_patterns: Vec<String> = Vec::new();
        // Progress event chain counter. BuildStarted declares Progress(0) as child.
        // Each Progress(N) declares Progress(N+1) as child, forming a chain.
        // The final Progress at CommandEnd adopts PatternExpanded events.
        let mut progress_count: i32 = 0;
        // Track whether we started a build and whether it completed normally.
        let mut build_started = false;
        let mut build_finished = false;
        // Track test results for emitting BEP TestResult + TestSummary.
        // Key: (label, config) → Vec of (test_name, passed)
        let mut test_results: HashMap<(String, String), Vec<(String, bool)>> = HashMap::new();
        stream! {
            for await event in events {
                match event.data() {
                    buck2_data::buck_event::Data::SpanStart(start) => {
                        match start.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_start_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_start::Data::Build(BuildCommandStart {}))
                                    | Some(buck2_data::command_start::Data::Test(buck2_data::TestCommandStart {})) => {
                                        // Lifecycle: BuildEnqueued
                                        yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::BuildEnqueued(
                                                v1::build_event::BuildEnqueued { details: None },
                                            )),
                                        });
                                        // Lifecycle: InvocationAttemptStarted
                                        yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::InvocationAttemptStarted(
                                                v1::build_event::InvocationAttemptStarted {
                                                    attempt_number: 1,
                                                    details: None,
                                                },
                                            )),
                                        });
                                        // BEP: BuildStarted
                                        // Children: BuildFinished, UnstructuredCommandLine, Progress(0)
                                        // Progress(0) serves as catch-all parent for PatternExpanded events
                                        // that arrive later once we know the target patterns.
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Started(build_event_stream::build_event_id::BuildStartedId {})) }),
                                            children: vec![
                                                BuildEventId { id: Some(build_event_id::Id::BuildFinished(build_event_id::BuildFinishedId {})) },
                                                BuildEventId { id: Some(build_event_id::Id::UnstructuredCommandLine(build_event_id::UnstructuredCommandLineId {})) },
                                                BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: 0 })) },
                                            ],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Started(build_event_stream::BuildStarted {
                                                uuid: event.event.trace_id.clone(),
                                                start_time: Some(event.timestamp().into()),
                                                build_tool_version: "BUCK2".to_owned(),
                                                options_description: "UNKNOWN".to_owned(),
                                                command: "build".to_owned(),
                                                working_directory: "UNKNOWN".to_owned(),
                                                workspace_directory: "UNKNOWN".to_owned(),
                                                server_pid: std::process::id() as i64,
                                                ..Default::default()
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        });
                                        // BEP: UnstructuredCommandLine
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(BuildEventId { id: Some(build_event_id::Id::UnstructuredCommandLine(build_event_id::UnstructuredCommandLineId {})) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::UnstructuredCommandLine(build_event_stream::UnstructuredCommandLine {
                                                args: command.cli_args.clone(),
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        });
                                        build_started = true;
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                                // Extract label and configuration from ConfiguredTargetLabel
                                let (label, config, rule) = match analysis.target.as_ref() {
                                    Some(buck2_data::analysis_start::Target::StandardTarget(ct)) => {
                                        let label = ct.label.as_ref().map(|l| bazel_label(&l.package, &l.name));
                                        let config = ct.configuration.as_ref().map(|c| c.full_name.clone());
                                        (label, config, analysis.rule.clone())
                                    }
                                    _ => (None, None, String::new()),
                                };
                                if let (Some(label), Some(config)) = (label, config) {
                                    configured_targets.push((label.clone(), config.clone(), rule.clone()));
                                    let is_new_config = emitted_configurations.insert(config.clone());
                                    // BEP: TargetConfigured — declares TargetCompleted as child,
                                    // and Configuration as child if this is a new config.
                                    let mut children = vec![
                                        BuildEventId { id: Some(build_event_id::Id::TargetCompleted(build_event_id::TargetCompletedId {
                                            label: label.clone(),
                                            configuration: Some(build_event_id::ConfigurationId { id: config.clone() }),
                                            aspect: "".to_owned(),
                                        }))},
                                    ];
                                    if is_new_config {
                                        children.push(BuildEventId { id: Some(build_event_id::Id::Configuration(build_event_id::ConfigurationId {
                                            id: config.clone(),
                                        }))});
                                    }
                                    let bes_event = build_event_stream::BuildEvent {
                                        id: Some(BuildEventId { id: Some(build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                            label: label.clone(),
                                            aspect: "".to_owned(),
                                        })) }),
                                        children,
                                        last_message: false,
                                        payload: Some(build_event_stream::build_event::Payload::Configured(build_event_stream::TargetConfigured {
                                            target_kind: if rule.is_empty() { "unknown rule type".to_owned() } else { rule },
                                            test_size: 0,
                                            tag: vec![],
                                        })),
                                    };
                                    let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                        type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                        value: bes_event.encode_to_vec(),
                                    });
                                    yield BesTransportEvent::Stream(v1::BuildEvent {
                                        event_time: Some(event.timestamp().into()),
                                        event: Some(bazel_event),
                                    });
                                    // BEP: Configuration — emitted once per unique configuration
                                    if is_new_config {
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(BuildEventId { id: Some(build_event_id::Id::Configuration(build_event_id::ConfigurationId {
                                                id: config,
                                            })) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Configuration(build_event_stream::Configuration {
                                                mnemonic: String::new(),
                                                platform_name: String::new(),
                                                cpu: String::new(),
                                                make_variable: Default::default(),
                                                is_tool: false,
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        });
                                    }
                                }
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::SpanEnd(end) => {
                        match end.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_end_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_end::Data::Build(_))
                                    | Some(buck2_data::command_end::Data::Test(_)) => {
                                        // BEP: Progress(0) — catch-all parent for PatternExpanded events.
                                        // Emitted here so it can declare all patterns as children.
                                        let pattern_children: Vec<BuildEventId> = if !parsed_patterns.is_empty() {
                                            parsed_patterns.iter().map(|p| {
                                                BuildEventId { id: Some(build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                    pattern: vec![p.clone()],
                                                }))}
                                            }).collect()
                                        } else {
                                            // Fallback: one PatternExpanded per configured target label
                                            configured_targets.iter().map(|(label, _, _)| {
                                                BuildEventId { id: Some(build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                    pattern: vec![label.clone()],
                                                }))}
                                            }).collect()
                                        };
                                        // Final Progress(N) in the chain — adopts all PatternExpanded events.
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count })) }),
                                            children: pattern_children,
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Progress(build_event_stream::Progress {
                                                stdout: String::new(),
                                                stderr: String::new(),
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        });

                                        // BEP: PatternExpanded events
                                        let all_target_configured_ids: Vec<BuildEventId> = configured_targets.iter().map(|(label, _, _)| {
                                            BuildEventId { id: Some(build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                                label: label.clone(),
                                                aspect: "".to_owned(),
                                            }))}
                                        }).collect();

                                        if !parsed_patterns.is_empty() {
                                            for pattern in parsed_patterns.iter() {
                                                let bes_event = build_event_stream::BuildEvent {
                                                    id: Some(BuildEventId { id: Some(build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                        pattern: vec![pattern.clone()],
                                                    })) }),
                                                    children: all_target_configured_ids.clone(),
                                                    last_message: false,
                                                    payload: Some(build_event_stream::build_event::Payload::Expanded(build_event_stream::PatternExpanded {
                                                        test_suite_expansions: vec![],
                                                    })),
                                                };
                                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                                    value: bes_event.encode_to_vec(),
                                                });
                                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                                    event_time: Some(event.timestamp().into()),
                                                    event: Some(bazel_event),
                                                });
                                            }
                                        } else {
                                            // Fallback: one PatternExpanded per target
                                            for (label, _, _) in configured_targets.iter() {
                                                let bes_event = build_event_stream::BuildEvent {
                                                    id: Some(BuildEventId { id: Some(build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                        pattern: vec![label.clone()],
                                                    })) }),
                                                    children: vec![
                                                        BuildEventId { id: Some(build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                                            label: label.clone(),
                                                            aspect: "".to_owned(),
                                                        }))},
                                                    ],
                                                    last_message: false,
                                                    payload: Some(build_event_stream::build_event::Payload::Expanded(build_event_stream::PatternExpanded {
                                                        test_suite_expansions: vec![],
                                                    })),
                                                };
                                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                                    value: bes_event.encode_to_vec(),
                                                });
                                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                                    event_time: Some(event.timestamp().into()),
                                                    event: Some(bazel_event),
                                                });
                                            }
                                        }

                                        // Emit TargetCompleted for ALL configured targets.
                                        // Targets without actions (cached, header-only, etc.)
                                        // still need TargetCompleted to fulfill the child
                                        // declared by TargetConfigured, otherwise BES servers
                                        // consider them "still building".
                                        for (label, config, _rule) in configured_targets.iter() {
                                            let actions = target_actions.remove(&(label.clone(), config.clone()));
                                            let success = actions.as_ref().map_or(true, |a| a.iter().all(|(_, s)| *s));
                                            let mut children: Vec<_> = actions.into_iter().flatten().map(|(id, _)| id).collect();
                                            let target_test_results = test_results.get(&(label.clone(), config.clone()));
                                            // Declare TestResult children
                                            if let Some(results) = target_test_results {
                                                for (i, _) in results.iter().enumerate() {
                                                    children.push(BuildEventId { id: Some(build_event_id::Id::TestResult(build_event_id::TestResultId {
                                                        label: label.clone(),
                                                        configuration: Some(build_event_id::ConfigurationId { id: config.clone() }),
                                                        run: (i + 1) as i32,
                                                        shard: 1,
                                                        attempt: 1,
                                                    })) });
                                                }
                                                // Declare TestSummary child
                                                children.push(BuildEventId { id: Some(build_event_id::Id::TestSummary(build_event_id::TestSummaryId {
                                                    label: label.clone(),
                                                    configuration: Some(build_event_id::ConfigurationId { id: config.clone() }),
                                                })) });
                                            }
                                            let bes_event = build_event_stream::BuildEvent {
                                                id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetCompleted(build_event_id::TargetCompletedId {
                                                    label: label.clone(),
                                                    configuration: Some(build_event_id::ConfigurationId { id: config.clone() }),
                                                    aspect: "".to_owned(),
                                                })) }),
                                                children,
                                                last_message: false,
                                                payload: Some(build_event_stream::build_event::Payload::Completed(build_event_stream::TargetComplete {
                                                    success,
                                                    output_group: vec![],
                                                    directory_output: vec![],
                                                    tag: vec![],
                                                    test_timeout: None,
                                                    failure_detail: None,
                                                    ..Default::default()
                                                })),
                                            };
                                            let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                                type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                                value: bes_event.encode_to_vec(),
                                            });
                                            yield BesTransportEvent::Stream(v1::BuildEvent {
                                                event_time: Some(event.timestamp().into()),
                                                event: Some(bazel_event),
                                            });
                                        }

                                        // Emit TestSummary for targets that had test results.
                                        for ((label, config), results) in test_results.iter() {
                                            let total = results.len() as i32;
                                            let passed = results.iter().filter(|(_, p)| *p).count() as i32;
                                            let overall_status = if passed == total {
                                                build_event_stream::TestStatus::Passed
                                            } else {
                                                build_event_stream::TestStatus::Failed
                                            };
                                            let bes_event = build_event_stream::BuildEvent {
                                                id: Some(BuildEventId { id: Some(build_event_id::Id::TestSummary(build_event_id::TestSummaryId {
                                                    label: label.clone(),
                                                    configuration: Some(build_event_id::ConfigurationId { id: config.clone() }),
                                                })) }),
                                                children: vec![],
                                                last_message: false,
                                                payload: Some(build_event_stream::build_event::Payload::TestSummary(build_event_stream::TestSummary {
                                                    overall_status: overall_status.into(),
                                                    total_run_count: total,
                                                    run_count: total,
                                                    attempt_count: 1,
                                                    shard_count: 1,
                                                    ..Default::default()
                                                })),
                                            };
                                            let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                                type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                                value: bes_event.encode_to_vec(),
                                            });
                                            yield BesTransportEvent::Stream(v1::BuildEvent {
                                                event_time: Some(event.timestamp().into()),
                                                event: Some(bazel_event),
                                            });
                                        }

                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::BuildFinished(build_event_stream::build_event_id::BuildFinishedId {})) }),
                                            children: vec![],
                                            last_message: true,
                                            payload: Some(build_event_stream::build_event::Payload::Finished(build_event_stream::BuildFinished {
                                                exit_code: Some(
                                                    if command.is_success {
                                                        build_event_stream::build_finished::ExitCode {
                                                            name: "SUCCESS".to_owned(),
                                                            code: 0,
                                                        }
                                                    } else {
                                                        build_event_stream::build_finished::ExitCode {
                                                            name: "FAILURE".to_owned(),
                                                            code: 1,
                                                        }
                                                    }),
                                                finish_time: Some(event.timestamp().into()),
                                                failure_detail: if command.is_success {
                                                    None
                                                } else {
                                                    Some(bazel_event_publisher_proto::failure_details::FailureDetail {
                                                        message: "Build failed".to_owned(),
                                                        category: None,
                                                    })
                                                },
                                                ..Default::default()
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        });
                                        // BES: BuildComponentStreamFinished
                                        yield BesTransportEvent::Stream(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::ComponentStreamFinished(
                                                v1::build_event::BuildComponentStreamFinished {
                                                    r#type: v1::build_event::build_component_stream_finished::FinishType::Finished.into(),
                                                },
                                            )),
                                        });
                                        // Lifecycle: InvocationAttemptFinished
                                        let result = if command.is_success {
                                            v1::build_status::Result::CommandSucceeded
                                        } else {
                                            v1::build_status::Result::CommandFailed
                                        };
                                        let status = v1::BuildStatus {
                                            result: result.into(),
                                            ..Default::default()
                                        };
                                        yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::InvocationAttemptFinished(
                                                v1::build_event::InvocationAttemptFinished {
                                                    invocation_status: Some(status.clone()),
                                                    details: None,
                                                },
                                            )),
                                        });
                                        // Lifecycle: BuildFinished
                                        yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::BuildFinished(
                                                v1::build_event::BuildFinished {
                                                    status: Some(status),
                                                    details: None,
                                                },
                                            )),
                                        });
                                        build_finished = true;
                                        break;
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_end_event::Data::ActionExecution(action)) => {
                                let configuration = match &action.key {
                                    None => None,
                                    Some(key) => match &key.owner {
                                        None => None,
                                        Some(owner) => match owner {
                                           buck2_data::action_key::Owner::TargetLabel(target) => target.configuration.clone(),
                                           buck2_data::action_key::Owner::TestTargetLabel(test) => test.configuration.clone(),
                                           buck2_data::action_key::Owner::LocalResourceSetup(resource) => resource.configuration.clone(),
                                           buck2_data::action_key::Owner::AnonTarget(_anon) => None, // TODO: execution configuration?
                                           buck2_data::action_key::Owner::BxlKey(_bxl) => None,
                                        },
                                    },
                                }.map(|configuration| build_event_id::ConfigurationId { id: configuration.full_name.clone() });
                                let label = match &action.key {
                                    None => None,
                                    Some(key) => match &key.owner {
                                        None => None,
                                        Some(owner) => match owner {
                                           buck2_data::action_key::Owner::TargetLabel(target) => target.label.clone(),
                                           buck2_data::action_key::Owner::TestTargetLabel(test) => test.label.clone(),
                                           buck2_data::action_key::Owner::LocalResourceSetup(resource) => resource.label.clone(),
                                           buck2_data::action_key::Owner::AnonTarget(anon) => anon.name.clone(),
                                           buck2_data::action_key::Owner::BxlKey(_bxl) => None, // TODO: handle bxl
                                        },
                                    },
                                }.map(|label| bazel_label(&label.package, &label.name));
                                let action_id = BuildEventId {id: Some(build_event_id::Id::ActionCompleted(build_event_id::ActionCompletedId {
                                    configuration: configuration.clone(),
                                    label: label.clone().unwrap_or("UNKNOWN".to_owned()),
                                    primary_output: "UNKNOWN".to_owned(),
                                }))};
                                let mnemonic = action.name.as_ref().map(|name| name.category.clone()).unwrap_or("UNKNOWN".to_owned());
                                let success = !action.failed;
                                let last_command_details = action.commands.last().and_then(|command| command.details.as_ref());
                                let command_line: Vec<String> = match last_command_details.and_then(|command| command.command_kind.as_ref()).and_then(|kind| kind.command.as_ref()) {
                                    None => vec![],
                                    Some(buck2_data::command_execution_kind::Command::LocalCommand(command)) => command.argv.clone(),
                                    Some(buck2_data::command_execution_kind::Command::RemoteCommand(command)) => {
                                        vec![format!("remote:{}", command.action_digest)]
                                    }
                                    Some(buck2_data::command_execution_kind::Command::WorkerCommand(command)) => command.argv.clone(),
                                    Some(buck2_data::command_execution_kind::Command::WorkerInitCommand(command)) => command.argv.clone(),
                                    Some(buck2_data::command_execution_kind::Command::OmittedLocalCommand(command)) => {
                                        vec![format!("omitted_local:{}", command.action_digest)]
                                    }
                                };
                                let exit_code = last_command_details.and_then(|details| details.signed_exit_code).unwrap_or(0);
                                let stdout = last_command_details.map(|details| details.cmd_stdout.clone());
                                let stderr = last_command_details.map(|details| details.cmd_stderr.clone());
                                let stdout_file = stdout.map(|stdout: String| bazel_event_publisher_proto::build_event_stream::File {
                                    path_prefix: vec![],
                                    name: "stdout".to_owned(),
                                    digest: "".to_owned(),
                                    length: stdout.len() as i64,
                                    file: Some(bazel_event_publisher_proto::build_event_stream::file::File::Contents(stdout.into())),
                                });
                                let stderr_file = stderr.clone().map(|stderr: String| bazel_event_publisher_proto::build_event_stream::File {
                                    path_prefix: vec![],
                                    name: "stderr".to_owned(),
                                    digest: "".to_owned(),
                                    length: stderr.len() as i64,
                                    file: Some(bazel_event_publisher_proto::build_event_stream::file::File::Contents(stderr.into())),
                                });
                                let start_time = last_command_details.and_then(|details| details.metadata.as_ref().and_then(|metadata| metadata.start_time.clone()));
                                let end_time = last_command_details.and_then(|details| {
                                    let meta = details.metadata.as_ref()?;
                                    let start = meta.start_time.as_ref()?;
                                    let wall = meta.wall_time.as_ref()?;
                                    Some(prost_types::Timestamp {
                                        seconds: start.seconds + wall.seconds + ((start.nanos as i64 + wall.nanos as i64) / 1_000_000_000),
                                        nanos: ((start.nanos as i64 + wall.nanos as i64) % 1_000_000_000) as i32,
                                    })
                                });
                                match (label.as_ref(), configuration.as_ref()) {
                                    (Some(label), Some(configuration)) => {
                                        target_actions
                                            .entry((label.clone(), configuration.id.clone()))
                                            .or_default()
                                            .push((action_id.clone(), success));
                                    },
                                    _ => {},
                                }
                                let failure_detail = if success { None } else {
                                    Some(bazel_event_publisher_proto::failure_details::FailureDetail {
                                        message: stderr.unwrap_or("UNKNOWN".to_owned()),
                                        category: None, // TODO
                                    })
                                };
                                let bes_event = build_event_stream::BuildEvent {
                                    id: Some(action_id),
                                    children: vec![],
                                    last_message: false,
                                    payload: Some(build_event_stream::build_event::Payload::Action(build_event_stream::ActionExecuted {
                                        success: success,
                                        r#type: mnemonic,
                                        exit_code: exit_code,
                                        stdout: stdout_file,
                                        stderr: stderr_file,
                                        primary_output: None,
                                        command_line: command_line,
                                        action_metadata_logs: vec![],
                                        failure_detail: failure_detail,
                                        start_time: start_time, // TODO: should we deduct queue time?
                                        end_time: end_time,
                                        strategy_details: vec![],
                                        ..Default::default()
                                    })),
                                };
                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                    value: bes_event.encode_to_vec(),
                                });
                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                });
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::Instant(instant) => {
                        match instant.data.as_ref() {
                            Some(buck2_data::instant_event::Data::TargetPatterns(patterns)) => {
                                parsed_patterns = patterns.target_patterns.iter().map(|p| p.value.clone()).collect();
                            }
                            Some(buck2_data::instant_event::Data::ConsoleMessage(msg)) => {
                                // BEP: Progress(N) — chains to Progress(N+1)
                                let bes_event = build_event_stream::BuildEvent {
                                    id: Some(BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count })) }),
                                    children: vec![
                                        BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count + 1 })) },
                                    ],
                                    last_message: false,
                                    payload: Some(build_event_stream::build_event::Payload::Progress(build_event_stream::Progress {
                                        stdout: String::new(),
                                        stderr: msg.message.clone(),
                                    })),
                                };
                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                    value: bes_event.encode_to_vec(),
                                });
                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                });
                                progress_count += 1;
                            }
                            Some(buck2_data::instant_event::Data::ConsoleWarning(msg)) => {
                                let bes_event = build_event_stream::BuildEvent {
                                    id: Some(BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count })) }),
                                    children: vec![
                                        BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count + 1 })) },
                                    ],
                                    last_message: false,
                                    payload: Some(build_event_stream::build_event::Payload::Progress(build_event_stream::Progress {
                                        stdout: String::new(),
                                        stderr: msg.message.clone(),
                                    })),
                                };
                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                    value: bes_event.encode_to_vec(),
                                });
                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                });
                                progress_count += 1;
                            }
                            Some(buck2_data::instant_event::Data::StreamingOutput(msg)) => {
                                let bes_event = build_event_stream::BuildEvent {
                                    id: Some(BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count })) }),
                                    children: vec![
                                        BuildEventId { id: Some(build_event_id::Id::Progress(build_event_id::ProgressId { opaque_count: progress_count + 1 })) },
                                    ],
                                    last_message: false,
                                    payload: Some(build_event_stream::build_event::Payload::Progress(build_event_stream::Progress {
                                        stdout: msg.message.clone(),
                                        stderr: String::new(),
                                    })),
                                };
                                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                    value: bes_event.encode_to_vec(),
                                });
                                yield BesTransportEvent::Stream(v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                });
                                progress_count += 1;
                            }
                            Some(buck2_data::instant_event::Data::TestResult(result)) => {
                                let (label, config) = match result.target_label.as_ref() {
                                    Some(ct) => {
                                        let label = ct.label.as_ref().map(|l| bazel_label(&l.package, &l.name));
                                        let config = ct.configuration.as_ref().map(|c| c.full_name.clone());
                                        (label, config)
                                    }
                                    None => (None, None),
                                };
                                if let (Some(label), Some(config)) = (label, config) {
                                    let passed = result.status == buck2_data::TestStatus::Pass as i32;
                                    let bep_status = if passed {
                                        build_event_stream::TestStatus::Passed
                                    } else {
                                        build_event_stream::TestStatus::Failed
                                    };
                                    let run = test_results.entry((label.clone(), config.clone())).or_default().len() as i32;
                                    test_results.entry((label.clone(), config.clone())).or_default().push((result.name.clone(), passed));

                                    let bes_event = build_event_stream::BuildEvent {
                                        id: Some(BuildEventId { id: Some(build_event_id::Id::TestResult(build_event_id::TestResultId {
                                            label: label.clone(),
                                            configuration: Some(build_event_id::ConfigurationId { id: config }),
                                            run: run + 1,
                                            shard: 1,
                                            attempt: 1,
                                        })) }),
                                        children: vec![],
                                        last_message: false,
                                        payload: Some(build_event_stream::build_event::Payload::TestResult(build_event_stream::TestResult {
                                            status: bep_status.into(),
                                            status_details: result.name.clone(),
                                            test_attempt_duration: result.duration.clone(),
                                            ..Default::default()
                                        })),
                                    };
                                    let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                        type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                        value: bes_event.encode_to_vec(),
                                    });
                                    yield BesTransportEvent::Stream(v1::BuildEvent {
                                        event_time: Some(event.timestamp().into()),
                                        event: Some(bazel_event),
                                    });
                                }
                            }
                            _ => {}
                        }
                    },
                    buck2_data::buck_event::Data::Record(_record) => {},
                }
            }
            // If the stream ended without a normal CommandEnd, emit Aborted + BuildFinished
            // so BES consumers know the build was interrupted.
            if build_started && !build_finished {
                // BEP: Aborted — explains why the build didn't complete
                let bes_event = build_event_stream::BuildEvent {
                    id: Some(BuildEventId { id: Some(build_event_id::Id::BuildFinished(build_event_id::BuildFinishedId {})) }),
                    children: vec![],
                    last_message: true,
                    payload: Some(build_event_stream::build_event::Payload::Aborted(build_event_stream::Aborted {
                        reason: build_event_stream::aborted::AbortReason::UserInterrupted.into(),
                        description: "Build interrupted".to_owned(),
                    })),
                };
                let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                    type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                    value: bes_event.encode_to_vec(),
                });
                yield BesTransportEvent::Stream(v1::BuildEvent {
                    event_time: None,
                    event: Some(bazel_event),
                });
                // ComponentStreamFinished
                yield BesTransportEvent::Stream(v1::BuildEvent {
                    event_time: None,
                    event: Some(v1::build_event::Event::ComponentStreamFinished(
                        v1::build_event::BuildComponentStreamFinished {
                            r#type: v1::build_event::build_component_stream_finished::FinishType::Finished.into(),
                        },
                    )),
                });
                // Lifecycle: InvocationAttemptFinished
                let status = v1::BuildStatus {
                    result: v1::build_status::Result::CommandFailed.into(),
                    ..Default::default()
                };
                yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                    event_time: None,
                    event: Some(v1::build_event::Event::InvocationAttemptFinished(
                        v1::build_event::InvocationAttemptFinished {
                            invocation_status: Some(status.clone()),
                            details: None,
                        },
                    )),
                });
                // Lifecycle: BuildFinished
                yield BesTransportEvent::Lifecycle(v1::BuildEvent {
                    event_time: None,
                    event: Some(v1::build_event::Event::BuildFinished(
                        v1::build_event::BuildFinished {
                            status: Some(status),
                            details: None,
                        },
                    )),
                });
            }
        }
    }

    async fn event_sink_loop(recv: UnboundedReceiver<Vec<BuckEvent>>, config: Arc<BesConfig>) -> anyhow::Result<()> {
        let mut handlers: HashMap<String, (UnboundedSender<BuckEvent>, tokio::task::JoinHandle<anyhow::Result<()>>)> = HashMap::new();
        let client = connect_build_event_server(&config).await?;
        let mut recv = UnboundedReceiverStream::new(recv)
            .flat_map(|v|stream::iter(v));
        let result_uri = config.result_url.clone();
        let project_id = config.project_id.clone();
        while let Some(event) = recv.next().await {
            let trace_id = event.event.trace_id.clone();
            // Check if an existing handler has failed and clean it up.
            if handlers.get(&trace_id).is_some_and(|(_, h)| h.is_finished()) {
                let (_, handle) = handlers.remove(&trace_id).unwrap();
                match handle.await {
                    Ok(Ok(())) => {},
                    Ok(Err(e)) => tracing::warn!("BES handler failed: {:#}", e),
                    Err(e) => tracing::warn!("BES handler panicked: {}", e),
                }
            }
            if let Some((send, _)) = handlers.get(&trace_id) {
                send.send(event).unwrap_or_else(|e| tracing::warn!("BES send failed: {:?}", e));
            } else {
                let (send, recv) = mpsc::unbounded_channel::<BuckEvent>();
                send.send(event).expect("just-created channel cannot be closed");
                let mut client = client.clone();
                let result_uri = result_uri.clone();
                let project_id = project_id.clone();
                let handler_trace_id = trace_id.clone();
                let handler = tokio::spawn(async move {
                    let trace_id = handler_trace_id;
                    let recv = UnboundedReceiverStream::new(recv);
                    let events = buck_to_bazel_events(recv);
                    tokio::pin!(events);

                    if let Some(result_uri) = result_uri.as_ref() {
                        tracing::info!("BES results: {}{}", &result_uri, &trace_id);
                    }

                    // Channel for feeding stream events to the gRPC streaming call.
                    let (stream_tx, stream_rx) = mpsc::unbounded_channel::<PublishBuildToolEventStreamRequest>();
                    let stream_rx = UnboundedReceiverStream::new(stream_rx);

                    // Start the streaming RPC concurrently so it consumes events
                    // as they are sent, rather than buffering everything.
                    let mut stream_client = client.clone();
                    let stream_handle = tokio::spawn(async move {
                        let response = stream_client
                            .publish_build_tool_event_stream(Request::new(stream_rx))
                            .await?;
                        let mut inbound = response.into_inner();
                        while let Some(_ack) = inbound.message().await? {
                            // TODO: match ACK sequence numbers and retry on failure.
                        }
                        Ok::<(), tonic::Status>(())
                    });

                    // Sequence counters per stream. BES has three separate streams:
                    // - Build stream: BuildEnqueued, BuildFinished (no invocation_id)
                    // - Invocation stream: InvocationAttemptStarted, InvocationAttemptFinished
                    // - Tool stream: all BEP events
                    let mut build_seq: i64 = 0;
                    let mut invocation_seq: i64 = 0;
                    let mut stream_seq: i64 = 0;

                    // StreamIds for the three streams.
                    let build_stream_id = StreamId {
                        build_id: trace_id.clone(),
                        invocation_id: String::new(),
                        component: 0, // UNKNOWN_COMPONENT
                    };
                    let invocation_stream_id = StreamId {
                        build_id: trace_id.clone(),
                        invocation_id: trace_id.clone(),
                        component: 0, // UNKNOWN_COMPONENT
                    };
                    let tool_stream_id = StreamId {
                        build_id: trace_id.clone(),
                        invocation_id: trace_id.clone(),
                        component: v1::stream_id::BuildComponent::Tool.into(),
                    };

                    // Helper to build a lifecycle request.
                    let make_lifecycle_req = |seq: i64, stream_id: StreamId, event: v1::BuildEvent| -> PublishLifecycleEventRequest {
                        PublishLifecycleEventRequest {
                            service_level: 0, // NONINTERACTIVE
                            build_event: Some(OrderedBuildEvent {
                                stream_id: Some(stream_id),
                                sequence_number: seq,
                                event: Some(event),
                            }),
                            stream_timeout: None,
                            notification_keywords: vec![],
                            project_id: project_id.clone(),
                            check_preceding_lifecycle_events_present: false,
                        }
                    };

                    // Process events from the converter, routing by transport tag.
                    // Final lifecycle events (InvocationAttemptFinished, BuildFinished)
                    // must be sent after the tool stream is fully closed and acknowledged,
                    // otherwise the server rejects them as writes to a finished invocation.
                    let mut final_lifecycle_events: Vec<v1::BuildEvent> = Vec::new();

                    while let Some(transport_event) = events.next().await {
                        match transport_event {
                            BesTransportEvent::Lifecycle(event) => {
                                // Buffer final lifecycle events to send after stream closes.
                                match event.event.as_ref() {
                                    Some(v1::build_event::Event::InvocationAttemptFinished(_))
                                    | Some(v1::build_event::Event::BuildFinished(_)) => {
                                        final_lifecycle_events.push(event);
                                    }
                                    _ => {
                                        // Send non-final lifecycle events immediately.
                                        let (seq, stream_id) = match event.event.as_ref() {
                                            Some(v1::build_event::Event::BuildEnqueued(_)) => {
                                                build_seq += 1;
                                                (build_seq, build_stream_id.clone())
                                            }
                                            _ => {
                                                invocation_seq += 1;
                                                (invocation_seq, invocation_stream_id.clone())
                                            }
                                        };
                                        client.publish_lifecycle_event(Request::new(
                                            make_lifecycle_req(seq, stream_id, event),
                                        )).await.map_err(|e| anyhow::anyhow!("lifecycle RPC failed: {}", e))?;
                                    }
                                }
                            }
                            BesTransportEvent::Stream(event) => {
                                stream_seq += 1;
                                let _ = stream_tx.send(PublishBuildToolEventStreamRequest {
                                    check_preceding_lifecycle_events_present: false,
                                    notification_keywords: vec![],
                                    ordered_build_event: Some(OrderedBuildEvent {
                                        stream_id: Some(tool_stream_id.clone()),
                                        sequence_number: stream_seq,
                                        event: Some(event),
                                    }),
                                    project_id: project_id.clone(),
                                });
                            }
                        }
                    }

                    // Close the tool stream and wait for all ACKs before sending
                    // final lifecycle events.
                    drop(stream_tx);
                    stream_handle.await
                        .map_err(|e| anyhow::anyhow!("BES stream task panicked: {}", e))?
                        .map_err(|e| anyhow::anyhow!("BES stream RPC failed: {}", e))?;

                    // Now send InvocationAttemptFinished and BuildFinished.
                    for event in final_lifecycle_events {
                        let (seq, stream_id) = match event.event.as_ref() {
                            Some(v1::build_event::Event::BuildFinished(_)) => {
                                build_seq += 1;
                                (build_seq, build_stream_id.clone())
                            }
                            _ => {
                                invocation_seq += 1;
                                (invocation_seq, invocation_stream_id.clone())
                            }
                        };
                        client.publish_lifecycle_event(Request::new(
                            make_lifecycle_req(seq, stream_id, event),
                        )).await.map_err(|e| anyhow::anyhow!("final lifecycle RPC failed: {}", e))?;
                    }

                    tracing::info!(
                        "BES: published {} lifecycle events ({} build + {} invocation) and {} stream events",
                        build_seq + invocation_seq, build_seq, invocation_seq, stream_seq,
                    );
                    if let Some(result_uri) = result_uri.as_ref() {
                        tracing::info!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    Ok(())
                });
                handlers.insert(trace_id, (send, handler));
            }
        }
        // Close send handles and await all handlers.
        let handlers: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = handlers.into_values().map(|(_, handler)|handler).collect();
        // TODO: handle retry.
        try_join_all(handlers).await?.into_iter().collect::<anyhow::Result<Vec<()>>>()?;
        Ok(())
    }

    impl RemoteEventSink {
        pub fn new(config: Arc<BesConfig>) -> anyhow::Result<Self> {
            let (send, recv) = mpsc::unbounded_channel::<Vec<BuckEvent>>();
            let handler = std::thread::Builder::new()
                .name("buck-event-producer".to_owned())
                .spawn({
                    move || {
                        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                        runtime.block_on(event_sink_loop(recv, config)).unwrap();
                    }
                }).context("spawning buck-event-producer thread")?;
            Ok(RemoteEventSink {
                _handler: handler,
                send,
            })
        }
        pub async fn send_now(&self, event: BuckEvent) {
            self.send_messages_now(vec![event]).await;
        }
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) {
            // TODO: does this make sense for BES? If so, implement send now variant.
            if let Err(err) = self.send.send(events) {
                // TODO: proper error handling
                dbg!(err);
            }
        }
        pub fn offer(&self, event: BuckEvent) {
            if let Err(err) = self.send.send(vec![event]) {
                // TODO: proper error handling
                dbg!(err);
            }
        }
    }

    #[async_trait]
    impl EventSink for RemoteEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    self.offer(event);
                }
                Event::CommandResult(..) => {},
                Event::PartialResult(..) => {},
            }
        }
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            EventSinkStats {
                successes: 0,
                failures_invalid_request: 0,
                failures_unauthorized: 0,
                failures_rate_limited: 0,
                failures_pushed_back: 0,
                failures_enqueue_failed: 0,
                failures_internal_error: 0,
                failures_timed_out: 0,
                failures_unknown: 0,
                buffered: 0,
                dropped: 0,
                bytes_written: 0,
            }
        }
    }

    #[derive(Default)]
    pub struct ScribeConfig {
        pub buffer_size: usize,
        pub retry_backoff: Duration,
        pub retry_attempts: usize,
        pub message_batch_size: Option<usize>,
        pub thrift_timeout: Duration,
    }

    #[cfg(test)]
    mod tests {
        use std::time::SystemTime;

        use futures::StreamExt;
        use prost::Message;

        use super::*;
        use crate::BuckEvent;
        use crate::TraceId;

        // --- Input event constructors ---

        fn buck_event(trace_id: &TraceId, data: buck2_data::buck_event::Data) -> BuckEvent {
            BuckEvent::new(SystemTime::now(), trace_id.dupe(), None, None, data)
        }

        fn command_start(data: Option<buck2_data::command_start::Data>) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Command(
                    buck2_data::CommandStart {
                        data,
                        metadata: Default::default(),
                        cli_args: vec![],
                        tags: vec![],
                    },
                )),
            })
        }

        fn build_start() -> buck2_data::buck_event::Data {
            command_start(Some(buck2_data::command_start::Data::Build(
                buck2_data::BuildCommandStart {},
            )))
        }

        fn build_end(is_success: bool) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::Command(
                    buck2_data::CommandEnd {
                        data: Some(buck2_data::command_end::Data::Build(
                            buck2_data::BuildCommandEnd {
                                unresolved_target_patterns: vec![],
                            },
                        )),
                        is_success,
                        build_result: None,
                    },
                )),
                stats: None,
                duration: None,
            })
        }

        fn instant_event() -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent { data: None })
        }

        // --- Output matching ---

        use super::BesTransportEvent;

        /// Describes what we expect from the converter output.
        /// Each variant is tagged with its expected transport (Lifecycle vs Stream).
        #[derive(Debug)]
        #[allow(dead_code)]
        enum Expected {
            // Lifecycle events (sent via PublishLifecycleEvent unary RPC)
            BuildEnqueued,
            InvocationAttemptStarted,
            InvocationAttemptFinished,
            LifecycleBuildFinished,
            // BEP stream events (sent via PublishBuildToolEventStream)
            Started,
            UnstructuredCommandLine,
            Progress,
            Finished { success: bool },
            TargetConfigured { label: String },
            PatternExpanded,
            ActionCompleted { label: String },
            TargetCompleted { label: String },
            ComponentStreamFinished,
        }

        impl Expected {
            fn is_lifecycle(&self) -> bool {
                matches!(
                    self,
                    Expected::BuildEnqueued
                        | Expected::InvocationAttemptStarted
                        | Expected::InvocationAttemptFinished
                        | Expected::LifecycleBuildFinished
                )
            }
        }

        /// Assert that an actual BesTransportEvent matches an Expected.
        fn assert_matches(actual: &BesTransportEvent, expected: &Expected) {
            // Check transport tag
            match (actual, expected.is_lifecycle()) {
                (BesTransportEvent::Lifecycle(_), true) => {}
                (BesTransportEvent::Stream(_), false) => {}
                (BesTransportEvent::Lifecycle(_), false) => {
                    panic!("expected Stream event, got Lifecycle: {:?}", expected);
                }
                (BesTransportEvent::Stream(_), true) => {
                    panic!("expected Lifecycle event, got Stream: {:?}", expected);
                }
            }

            let event = match actual {
                BesTransportEvent::Lifecycle(e) | BesTransportEvent::Stream(e) => e,
            };
            let inner = event.event.as_ref().unwrap();

            match expected {
                // Lifecycle events: check the v1::BuildEvent variant directly
                Expected::BuildEnqueued => {
                    assert!(
                        matches!(inner, v1::build_event::Event::BuildEnqueued(_)),
                        "expected BuildEnqueued, got {:?}", inner,
                    );
                }
                Expected::InvocationAttemptStarted => {
                    assert!(
                        matches!(inner, v1::build_event::Event::InvocationAttemptStarted(_)),
                        "expected InvocationAttemptStarted, got {:?}", inner,
                    );
                }
                Expected::InvocationAttemptFinished => {
                    assert!(
                        matches!(inner, v1::build_event::Event::InvocationAttemptFinished(_)),
                        "expected InvocationAttemptFinished, got {:?}", inner,
                    );
                }
                Expected::LifecycleBuildFinished => {
                    assert!(
                        matches!(inner, v1::build_event::Event::BuildFinished(_)),
                        "expected lifecycle BuildFinished, got {:?}", inner,
                    );
                }
                Expected::ComponentStreamFinished => {
                    assert!(
                        matches!(inner, v1::build_event::Event::ComponentStreamFinished(_)),
                        "expected ComponentStreamFinished, got {:?}", inner,
                    );
                }
                // BEP events: decode the inner build_event_stream::BuildEvent
                _ => {
                    let any = match inner {
                        v1::build_event::Event::BazelEvent(any) => any,
                        other => panic!("expected BazelEvent, got {:?}", other),
                    };
                    let bes = build_event_stream::BuildEvent::decode(any.value.as_slice()).unwrap();
                    let id = bes.id.as_ref().and_then(|id| id.id.as_ref());
                    match expected {
                        Expected::Started => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::Started(_))),
                                "expected Started, got {:?}", id,
                            );
                        }
                        Expected::UnstructuredCommandLine => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::UnstructuredCommandLine(_))),
                                "expected UnstructuredCommandLine, got {:?}", id,
                            );
                        }
                        Expected::Progress => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::Progress(_))),
                                "expected Progress, got {:?}", id,
                            );
                        }
                        Expected::Finished { success } => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::BuildFinished(_))),
                                "expected BuildFinished, got {:?}", id,
                            );
                            assert!(bes.last_message, "BuildFinished should be last_message");
                            let finished = match bes.payload.as_ref().unwrap() {
                                build_event_stream::build_event::Payload::Finished(f) => f,
                                other => panic!("expected Finished payload, got {:?}", other),
                            };
                            let code = finished.exit_code.as_ref().unwrap();
                            if *success {
                                assert_eq!(code.code, 0);
                            } else {
                                assert_ne!(code.code, 0);
                            }
                        }
                        Expected::TargetConfigured { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::TargetConfigured(tc)) => {
                                    assert_eq!(&tc.label, label, "TargetConfigured label mismatch");
                                }
                                _ => panic!("expected TargetConfigured, got {:?}", id),
                            }
                        }
                        Expected::PatternExpanded => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::Pattern(_))),
                                "expected Pattern, got {:?}", id,
                            );
                        }
                        Expected::ActionCompleted { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::ActionCompleted(ac)) => {
                                    assert_eq!(&ac.label, label, "ActionCompleted label mismatch");
                                }
                                _ => panic!("expected ActionCompleted, got {:?}", id),
                            }
                        }
                        Expected::TargetCompleted { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::TargetCompleted(tc)) => {
                                    assert_eq!(&tc.label, label, "TargetCompleted label mismatch");
                                }
                                _ => panic!("expected TargetCompleted, got {:?}", id),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }

        /// Run a list of input BuckEvents through the converter and assert the
        /// output matches the expected events (both lifecycle and stream).
        async fn check(inputs: Vec<BuckEvent>, expected: Vec<Expected>) {
            let stream = tokio_stream::iter(inputs);
            let actual: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_eq!(
                actual.len(),
                expected.len(),
                "expected {} events, got {}",
                expected.len(),
                actual.len(),
            );
            for (actual, expected) in actual.iter().zip(expected.iter()) {
                assert_matches(actual, expected);
            }
        }

        // ====================================================
        // Test table: input Buck events → expected BES events
        // ====================================================

        #[tokio::test]
        async fn test_empty_stream() {
            check(vec![], vec![]).await;
        }

        #[tokio::test]
        async fn test_successful_build() {
            let t = TraceId::new();
            check(
                vec![
                    buck_event(&t, build_start()),
                    buck_event(&t, build_end(true)),
                ],
                vec![
                    Expected::BuildEnqueued,
                    Expected::InvocationAttemptStarted,
                    Expected::Started,
                    Expected::UnstructuredCommandLine,
                    Expected::Progress,
                    Expected::Finished { success: true },
                    Expected::ComponentStreamFinished,
                    Expected::InvocationAttemptFinished,
                    Expected::LifecycleBuildFinished,
                ],
            ).await;
        }

        #[tokio::test]
        async fn test_failed_build() {
            let t = TraceId::new();
            check(
                vec![
                    buck_event(&t, build_start()),
                    buck_event(&t, build_end(false)),
                ],
                vec![
                    Expected::BuildEnqueued,
                    Expected::InvocationAttemptStarted,
                    Expected::Started,
                    Expected::UnstructuredCommandLine,
                    Expected::Progress,
                    Expected::Finished { success: false },
                    Expected::ComponentStreamFinished,
                    Expected::InvocationAttemptFinished,
                    Expected::LifecycleBuildFinished,
                ],
            ).await;
        }

        #[tokio::test]
        async fn test_non_build_command_produces_nothing() {
            let t = TraceId::new();
            check(
                vec![buck_event(&t, command_start(None))],
                vec![],
            ).await;
        }

        #[tokio::test]
        async fn test_instant_and_record_events_produce_nothing() {
            let t = TraceId::new();
            check(
                vec![
                    buck_event(&t, instant_event()),
                    buck_event(&t, buck2_data::buck_event::Data::Record(
                        buck2_data::RecordEvent { data: None },
                    )),
                ],
                vec![],
            ).await;
        }

        // --- Additional input event constructors ---

        fn analysis_start(label: &str, config: &str, rule: &str) -> buck2_data::buck_event::Data {
            let (package, name) = label.split_once(':').unwrap();
            buck2_data::buck_event::Data::SpanStart(buck2_data::SpanStartEvent {
                data: Some(buck2_data::span_start_event::Data::Analysis(
                    buck2_data::AnalysisStart {
                        target: Some(buck2_data::analysis_start::Target::StandardTarget(
                            buck2_data::ConfiguredTargetLabel {
                                label: Some(buck2_data::TargetLabel {
                                    package: package.to_owned(),
                                    name: name.to_owned(),
                                }),
                                configuration: Some(buck2_data::Configuration {
                                    full_name: config.to_owned(),
                                }),
                                execution_configuration: None,
                            },
                        )),
                        rule: rule.to_owned(),
                    },
                )),
            })
        }

        fn action_execution_end(label: &str, config: &str, failed: bool) -> buck2_data::buck_event::Data {
            let (package, name) = label.split_once(':').unwrap();
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::ActionExecution(
                    Box::new(buck2_data::ActionExecutionEnd {
                        key: Some(buck2_data::ActionKey {
                            id: vec![],
                            key: "".to_owned(),
                            owner: Some(buck2_data::action_key::Owner::TargetLabel(
                                buck2_data::ConfiguredTargetLabel {
                                    label: Some(buck2_data::TargetLabel {
                                        package: package.to_owned(),
                                        name: name.to_owned(),
                                    }),
                                    configuration: Some(buck2_data::Configuration {
                                        full_name: config.to_owned(),
                                    }),
                                    execution_configuration: None,
                                },
                            )),
                        }),
                        name: Some(buck2_data::ActionName {
                            category: "cxx_compile".to_owned(),
                            identifier: "main.cpp".to_owned(),
                        }),
                        failed: failed,
                        commands: vec![],
                        outputs: vec![],
                        error_diagnostics: None,
                        eligible_for_full_hybrid: Some(false),
                        ..Default::default()
                    }),
                )),
                stats: None,
                duration: None,
            })
        }

        fn parsed_target_patterns(patterns: &[&str]) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::TargetPatterns(
                    buck2_data::ParsedTargetPatterns {
                        target_patterns: patterns.iter().map(|p| buck2_data::TargetPattern {
                            value: p.to_string(),
                        }).collect(),
                    },
                )),
            })
        }

        // --- BEP event graph (DAG) validation ---

        /// Extract BEP event IDs and children from stream events.
        /// Returns (event_ids, declared_children) where each is a set of
        /// serialized BuildEventId bytes for comparison.
        fn extract_bep_graph(events: &[BesTransportEvent]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
            let mut event_ids: Vec<Vec<u8>> = Vec::new();
            let mut declared_children: Vec<Vec<u8>> = Vec::new();

            for transport_event in events {
                let v1_event = match transport_event {
                    BesTransportEvent::Stream(e) => e,
                    BesTransportEvent::Lifecycle(_) => continue,
                };
                let inner = v1_event.event.as_ref().unwrap();
                let any = match inner {
                    v1::build_event::Event::BazelEvent(any) => any,
                    _ => continue, // ComponentStreamFinished etc.
                };
                let bes = build_event_stream::BuildEvent::decode(any.value.as_slice()).unwrap();
                if let Some(id) = &bes.id {
                    event_ids.push(id.encode_to_vec());
                }
                for child in &bes.children {
                    declared_children.push(child.encode_to_vec());
                }
            }
            (event_ids, declared_children)
        }

        /// Assert that the BEP event graph forms a valid DAG:
        /// 1. Every declared child must appear as an emitted event.
        /// 2. Every emitted event (except the root BuildStarted) must be
        ///    declared as a child by some other event.
        fn assert_valid_bep_dag(events: &[BesTransportEvent]) {
            use std::collections::HashSet;

            let (event_ids, declared_children) = extract_bep_graph(events);
            let event_set: HashSet<&[u8]> = event_ids.iter().map(|v| v.as_slice()).collect();
            let child_set: HashSet<&[u8]> = declared_children.iter().map(|v| v.as_slice()).collect();

            // Every declared child must have a corresponding emitted event.
            for child_bytes in &declared_children {
                if !event_set.contains(child_bytes.as_slice()) {
                    let child_id = build_event_stream::BuildEventId::decode(child_bytes.as_slice()).unwrap();
                    panic!("Declared child was never emitted: {:?}", child_id);
                }
            }

            // Every emitted event (except root) must be declared as a child.
            let root_id = BuildEventId {
                id: Some(build_event_id::Id::Started(build_event_id::BuildStartedId {})),
            };
            let root_bytes = root_id.encode_to_vec();
            for event_bytes in &event_ids {
                if event_bytes.as_slice() == root_bytes.as_slice() {
                    continue; // Root is not a child of anything
                }
                if !child_set.contains(event_bytes.as_slice()) {
                    let orphan_id = build_event_stream::BuildEventId::decode(event_bytes.as_slice()).unwrap();
                    panic!("Orphan event (not declared as child of any event): {:?}", orphan_id);
                }
            }
        }

        // --- DAG validation tests ---

        #[tokio::test]
        async fn test_dag_empty_build() {
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);
        }

        #[test]
        fn test_bazel_label_conversion() {
            // Buck2 cell-prefixed labels get @ prepended
            assert_eq!(bazel_label("root//foo", "bar"), "@root//foo:bar");
            assert_eq!(bazel_label("cell//pkg/sub", "target"), "@cell//pkg/sub:target");
            // Already Bazel-style labels are left as-is
            assert_eq!(bazel_label("//foo", "bar"), "//foo:bar");
            assert_eq!(bazel_label("@repo//foo", "bar"), "@repo//foo:bar");
        }

        #[tokio::test]
        async fn test_dag_build_with_targets() {
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo/..."])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, analysis_start("root//foo:baz", "cfg//linux-x86_64", "rust_library")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("root//foo:baz", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);
        }

        #[tokio::test]
        async fn test_dag_build_with_targets_no_patterns() {
            // When ParsedTargetPatterns is not sent, fallback to per-target patterns
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);
        }

        #[tokio::test]
        async fn test_dag_build_with_multiple_patterns() {
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar", "root//foo:baz"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, analysis_start("root//foo:baz", "cfg//linux-x86_64", "rust_library")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("root//foo:baz", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);
        }

        /// Create an ActionExecutionEnd with a command execution (local or remote).
        fn action_execution_end_with_command(
            label: &str,
            config: &str,
            failed: bool,
            command: buck2_data::CommandExecution,
        ) -> buck2_data::buck_event::Data {
            let (package, name) = label.split_once(':').unwrap();
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::ActionExecution(
                    Box::new(buck2_data::ActionExecutionEnd {
                        key: Some(buck2_data::ActionKey {
                            id: vec![],
                            key: "".to_owned(),
                            owner: Some(buck2_data::action_key::Owner::TargetLabel(
                                buck2_data::ConfiguredTargetLabel {
                                    label: Some(buck2_data::TargetLabel {
                                        package: package.to_owned(),
                                        name: name.to_owned(),
                                    }),
                                    configuration: Some(buck2_data::Configuration {
                                        full_name: config.to_owned(),
                                    }),
                                    execution_configuration: None,
                                },
                            )),
                        }),
                        name: Some(buck2_data::ActionName {
                            category: "cxx_compile".to_owned(),
                            identifier: "main.cpp".to_owned(),
                        }),
                        failed,
                        commands: vec![command],
                        outputs: vec![],
                        error_diagnostics: None,
                        eligible_for_full_hybrid: Some(false),
                        ..Default::default()
                    }),
                )),
                stats: None,
                duration: None,
            })
        }

        /// Create a CommandExecution with LocalCommand and details.
        fn local_command_execution(
            argv: Vec<String>,
            exit_code: i32,
            stdout: &str,
            stderr: &str,
            success: bool,
        ) -> buck2_data::CommandExecution {
            buck2_data::CommandExecution {
                details: Some(buck2_data::CommandExecutionDetails {
                    signed_exit_code: Some(exit_code),
                    cmd_stdout: stdout.to_owned(),
                    cmd_stderr: stderr.to_owned(),
                    command_kind: Some(buck2_data::CommandExecutionKind {
                        command: Some(buck2_data::command_execution_kind::Command::LocalCommand(
                            buck2_data::LocalCommand {
                                argv,
                                env: vec![],
                                action_digest: "".to_owned(),
                            },
                        )),
                    }),
                    metadata: None,
                    additional_message: None,
                }),
                status: if success {
                    Some(buck2_data::command_execution::Status::Success(
                        buck2_data::command_execution::Success {},
                    ))
                } else {
                    Some(buck2_data::command_execution::Status::Failure(
                        buck2_data::command_execution::Failure {},
                    ))
                },
                inline_environment_metadata: None,
            }
        }

        /// Create a CommandExecution with RemoteCommand.
        fn remote_command_execution(
            exit_code: i32,
            stdout: &str,
            stderr: &str,
            success: bool,
        ) -> buck2_data::CommandExecution {
            buck2_data::CommandExecution {
                details: Some(buck2_data::CommandExecutionDetails {
                    signed_exit_code: Some(exit_code),
                    cmd_stdout: stdout.to_owned(),
                    cmd_stderr: stderr.to_owned(),
                    command_kind: Some(buck2_data::CommandExecutionKind {
                        command: Some(buck2_data::command_execution_kind::Command::RemoteCommand(
                            buck2_data::RemoteCommand {
                                action_digest: "abc123".to_owned(),
                                cache_hit: false,
                                queue_time: None,
                                details: None,
                                cache_hit_type: 0,
                                remote_dep_file_key: None,
                                materialized_inputs_for_failed: vec![],
                                materialized_outputs_for_failed_actions: vec![],
                            },
                        )),
                    }),
                    metadata: None,
                    additional_message: None,
                }),
                status: if success {
                    Some(buck2_data::command_execution::Status::Success(
                        buck2_data::command_execution::Success {},
                    ))
                } else {
                    Some(buck2_data::command_execution::Status::Failure(
                        buck2_data::command_execution::Failure {},
                    ))
                },
                inline_environment_metadata: None,
            }
        }

        /// Create a ConsoleWarning instant event.
        fn console_warning(msg: &str) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::ConsoleWarning(
                    buck2_data::ConsoleWarning {
                        message: msg.to_owned(),
                    },
                )),
            })
        }

        /// Create a StreamingOutput instant event.
        fn streaming_output(msg: &str) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::StreamingOutput(
                    buck2_data::StdoutStreamingOutput {
                        message: msg.to_owned(),
                    },
                )),
            })
        }

        /// Create a TestCommandStart event.
        fn test_command_start() -> buck2_data::buck_event::Data {
            command_start(Some(buck2_data::command_start::Data::Test(
                buck2_data::TestCommandStart {},
            )))
        }

        /// Create a TestCommandEnd event.
        fn test_command_end(is_success: bool) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::SpanEnd(buck2_data::SpanEndEvent {
                data: Some(buck2_data::span_end_event::Data::Command(
                    buck2_data::CommandEnd {
                        data: Some(buck2_data::command_end::Data::Test(
                            buck2_data::TestCommandEnd {
                                unresolved_target_patterns: vec![],
                            },
                        )),
                        is_success,
                        build_result: None,
                    },
                )),
                stats: None,
                duration: None,
            })
        }

        /// Create a TestResult instant event.
        fn test_result(name: &str, label: &str, status: i32) -> buck2_data::buck_event::Data {
            let (package, target_name) = label.split_once(':').unwrap();
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::TestResult(
                    buck2_data::TestResult {
                        name: name.to_owned(),
                        status,
                        msg: None,
                        duration: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        }),
                        details: "".to_owned(),
                        target_label: Some(buck2_data::ConfiguredTargetLabel {
                            label: Some(buck2_data::TargetLabel {
                                package: package.to_owned(),
                                name: target_name.to_owned(),
                            }),
                            configuration: Some(buck2_data::Configuration {
                                full_name: "cfg//linux-x86_64".to_owned(),
                            }),
                            execution_configuration: None,
                        }),
                        max_memory_used_bytes: None,
                    },
                )),
            })
        }

        /// Extract decoded BEP BuildEvents from the stream (skipping lifecycle and non-BazelEvent).
        fn extract_bep_events(events: &[BesTransportEvent]) -> Vec<build_event_stream::BuildEvent> {
            events.iter().filter_map(|e| {
                let v1_event = match e {
                    BesTransportEvent::Stream(e) => e,
                    _ => return None,
                };
                let any = match v1_event.event.as_ref()? {
                    v1::build_event::Event::BazelEvent(any) => any,
                    _ => return None,
                };
                build_event_stream::BuildEvent::decode(any.value.as_slice()).ok()
            }).collect()
        }

        /// Collect all events from the converter for a given input.
        async fn collect_events(inputs: Vec<BuckEvent>) -> Vec<BesTransportEvent> {
            let stream = tokio_stream::iter(inputs);
            buck_to_bazel_events(stream).collect().await
        }

        fn console_message(msg: &str) -> buck2_data::buck_event::Data {
            buck2_data::buck_event::Data::Instant(buck2_data::InstantEvent {
                data: Some(buck2_data::instant_event::Data::ConsoleMessage(
                    buck2_data::ConsoleMessage {
                        message: msg.to_owned(),
                    },
                )),
            })
        }

        #[tokio::test]
        async fn test_dag_build_with_progress() {
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, console_message("Analyzing target root//foo:bar")),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, console_message("Building root//foo:bar")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);

            // Verify the Progress chain has the right content
            let progress_events: Vec<_> = events.iter().filter_map(|e| {
                let v1_event = match e {
                    BesTransportEvent::Stream(e) => e,
                    _ => return None,
                };
                let any = match v1_event.event.as_ref()? {
                    v1::build_event::Event::BazelEvent(any) => any,
                    _ => return None,
                };
                let bes = build_event_stream::BuildEvent::decode(any.value.as_slice()).ok()?;
                match bes.id.as_ref()?.id.as_ref()? {
                    build_event_id::Id::Progress(p) => {
                        let stderr = match bes.payload.as_ref()? {
                            build_event_stream::build_event::Payload::Progress(p) => p.stderr.clone(),
                            _ => String::new(),
                        };
                        Some((p.opaque_count, stderr))
                    }
                    _ => None,
                }
            }).collect();

            // Progress(0) and Progress(1) carry console messages,
            // Progress(2) is the final one at CommandEnd (empty stderr, adopts patterns).
            assert_eq!(progress_events.len(), 3);
            assert_eq!(progress_events[0], (0, "Analyzing target root//foo:bar".to_owned()));
            assert_eq!(progress_events[1], (1, "Building root//foo:bar".to_owned()));
            assert_eq!(progress_events[2].0, 2);
            assert!(progress_events[2].1.is_empty());
        }

        // ================================================================
        // Priority 1: Fill gaps in existing conversion logic
        // ================================================================

        /// Test that actions with RemoteCommand still emit ActionCompleted.
        /// Currently the converter only extracts command_line from LocalCommand
        /// and falls back to empty vec for remote — verify the action is still
        /// emitted and the DAG is valid.
        #[tokio::test]
        async fn test_action_with_remote_command() {
            let t = TraceId::new();
            let cmd = remote_command_execution(0, "", "built successfully", true);
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end_with_command("root//foo:bar", "cfg//linux-x86_64", false, cmd)),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            // Find the ActionCompleted event and verify it was emitted
            let bep = extract_bep_events(&events);
            let action = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::ActionCompleted(_)),
            )).expect("ActionCompleted should be emitted for remote commands");

            // Remote commands should have their argv extracted, just like local commands.
            match action.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Action(a) => {
                    assert!(a.success);
                    // Remote commands expose action_digest, not argv, but the
                    // converter should still populate command_line from whatever
                    // is available (e.g. the original command args).
                    // At minimum, remote actions should not silently drop all
                    // command info — assert that something is present.
                    assert!(
                        !a.command_line.is_empty(),
                        "remote commands should have command_line populated (action_digest, etc.)",
                    );
                }
                other => panic!("expected Action payload, got {:?}", other),
            }
        }

        /// Test that a failed action's stderr propagates into ActionExecuted.failure_detail.
        #[tokio::test]
        async fn test_action_with_failure_details() {
            let t = TraceId::new();
            let cmd = local_command_execution(
                vec!["gcc".to_owned(), "-c".to_owned(), "main.cpp".to_owned()],
                1,
                "",
                "error: undefined reference to 'main'",
                false,
            );
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end_with_command("root//foo:bar", "cfg//linux-x86_64", true, cmd)),
                buck_event(&t, build_end(false)),
            ]).await;

            let bep = extract_bep_events(&events);
            let action = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::ActionCompleted(_)),
            )).expect("ActionCompleted should be emitted");

            match action.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Action(a) => {
                    assert!(!a.success, "action should be marked as failed");
                    assert_eq!(a.exit_code, 1);
                    // Verify failure_detail contains the stderr
                    let fd = a.failure_detail.as_ref().expect("failed action should have failure_detail");
                    assert!(
                        fd.message.contains("undefined reference"),
                        "failure_detail.message should contain stderr, got: {}",
                        fd.message,
                    );
                    // Verify stderr file is also present
                    let stderr_file = a.stderr.as_ref().expect("stderr file should be present");
                    assert_eq!(stderr_file.name, "stderr");
                }
                other => panic!("expected Action payload, got {:?}", other),
            }
        }

        /// Test that BuildFinished.failure_detail is populated for failed builds.
        #[tokio::test]
        async fn test_build_finished_failure_detail() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, build_end(false)),
            ]).await;

            let bep = extract_bep_events(&events);
            let finished = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::BuildFinished(_)),
            )).expect("BuildFinished should be emitted");

            match finished.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Finished(f) => {
                    assert_eq!(f.exit_code.as_ref().unwrap().code, 1);
                    let fd = f.failure_detail.as_ref()
                        .expect("failed build should have failure_detail");
                    assert!(!fd.message.is_empty(), "failure_detail.message should not be empty");
                }
                other => panic!("expected Finished payload, got {:?}", other),
            }
        }

        /// Test that successful builds do NOT have failure_detail.
        #[tokio::test]
        async fn test_build_finished_success_no_failure_detail() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, build_end(true)),
            ]).await;

            let bep = extract_bep_events(&events);
            let finished = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::BuildFinished(_)),
            )).expect("BuildFinished should be emitted");

            match finished.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Finished(f) => {
                    assert_eq!(f.exit_code.as_ref().unwrap().code, 0);
                    assert!(f.failure_detail.is_none(), "successful build should not have failure_detail");
                }
                other => panic!("expected Finished payload, got {:?}", other),
            }
        }

        /// Test that ConsoleWarning instant events produce Progress events with stderr.
        #[tokio::test]
        async fn test_console_warning_produces_progress() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, console_warning("WARNING: deprecated rule used")),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            let progress_events: Vec<_> = bep.iter().filter_map(|e| {
                match e.id.as_ref()?.id.as_ref()? {
                    build_event_id::Id::Progress(p) => {
                        let (stdout, stderr) = match e.payload.as_ref()? {
                            build_event_stream::build_event::Payload::Progress(p) => (p.stdout.clone(), p.stderr.clone()),
                            _ => (String::new(), String::new()),
                        };
                        Some((p.opaque_count, stdout, stderr))
                    }
                    _ => None,
                }
            }).collect();

            // Progress(0) from warning, Progress(1) from CommandEnd
            assert!(progress_events.len() >= 2, "expected at least 2 progress events, got {}", progress_events.len());
            assert_eq!(progress_events[0].0, 0);
            assert_eq!(progress_events[0].2, "WARNING: deprecated rule used");
            assert!(progress_events[0].1.is_empty(), "warning should go to stderr, not stdout");
        }

        /// Test that StreamingOutput instant events produce Progress events with stdout.
        #[tokio::test]
        async fn test_streaming_output_produces_progress() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, streaming_output("test output line 1\n")),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            let progress_events: Vec<_> = bep.iter().filter_map(|e| {
                match e.id.as_ref()?.id.as_ref()? {
                    build_event_id::Id::Progress(p) => {
                        let (stdout, stderr) = match e.payload.as_ref()? {
                            build_event_stream::build_event::Payload::Progress(p) => (p.stdout.clone(), p.stderr.clone()),
                            _ => (String::new(), String::new()),
                        };
                        Some((p.opaque_count, stdout, stderr))
                    }
                    _ => None,
                }
            }).collect();

            assert!(progress_events.len() >= 2, "expected at least 2 progress events, got {}", progress_events.len());
            assert_eq!(progress_events[0].0, 0);
            assert_eq!(progress_events[0].1, "test output line 1\n");
            assert!(progress_events[0].2.is_empty(), "streaming output should go to stdout, not stderr");
        }

        /// Test that action start_time is set from command execution metadata.
        #[tokio::test]
        async fn test_action_timing_fields() {
            let t = TraceId::new();
            let mut cmd = local_command_execution(
                vec!["gcc".to_owned()],
                0, "", "", true,
            );
            // Set metadata with start_time
            if let Some(ref mut details) = cmd.details {
                details.metadata = Some(buck2_data::CommandExecutionMetadata {
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1700000000,
                        nanos: 0,
                    }),
                    wall_time: Some(prost_types::Duration {
                        seconds: 5,
                        nanos: 0,
                    }),
                    ..Default::default()
                });
            }
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end_with_command("root//foo:bar", "cfg//linux-x86_64", false, cmd)),
                buck_event(&t, build_end(true)),
            ]).await;

            let bep = extract_bep_events(&events);
            let action = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::ActionCompleted(_)),
            )).expect("ActionCompleted should be emitted");

            match action.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Action(a) => {
                    let start = a.start_time.as_ref().expect("start_time should be set from metadata");
                    assert_eq!(start.seconds, 1700000000, "start_time should match metadata");
                    // end_time should be start_time + wall_time
                    let end = a.end_time.as_ref().expect("end_time should be computed from start_time + wall_time");
                    assert_eq!(end.seconds, 1700000005, "end_time should be start_time(1700000000) + wall_time(5s)");
                }
                other => panic!("expected Action payload, got {:?}", other),
            }
        }

        // ================================================================
        // Priority 2: Robustness and edge cases
        // ================================================================

        /// Test that a target with 3+ actions has all actions as children of TargetCompleted.
        #[tokio::test]
        async fn test_target_with_multiple_actions() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            // Find TargetCompleted and check it has 3 ActionCompleted children
            let target_completed = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetCompleted(_)),
            )).expect("TargetCompleted should be emitted");

            let action_children: Vec<_> = target_completed.children.iter().filter(|c| matches!(
                c.id.as_ref(),
                Some(build_event_id::Id::ActionCompleted(_)),
            )).collect();
            assert_eq!(action_children.len(), 3, "TargetCompleted should declare 3 ActionCompleted children");
        }

        /// Test what happens when ActionExecutionEnd arrives for a target that never
        /// had AnalysisStart — the action should still be emitted as an orphan is
        /// caught by DAG validation.
        #[tokio::test]
        async fn test_target_with_no_analysis() {
            let t = TraceId::new();
            // ActionExecutionEnd without preceding AnalysisStart for that target
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]).await;

            // The ActionCompleted event should exist but the DAG may be invalid
            // because there's no TargetConfigured → TargetCompleted chain to parent it.
            let bep = extract_bep_events(&events);
            let has_action = bep.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::ActionCompleted(_)),
            ));
            assert!(has_action, "ActionCompleted should still be emitted even without AnalysisStart");

            // DAG validation: the ActionCompleted will be an orphan since there's no
            // TargetCompleted to parent it (no AnalysisStart → no configured_targets entry).
            // This documents a gap in the current implementation.
            let result = std::panic::catch_unwind(|| assert_valid_bep_dag(&events));
            // We expect this to panic because the action is orphaned — no TargetCompleted
            // exists to declare it as a child.
            assert!(result.is_err(), "DAG should be invalid: ActionCompleted is orphaned without AnalysisStart");
        }

        /// Test that a target with a mix of successful and failed actions
        /// has TargetCompleted.success = false.
        #[tokio::test]
        async fn test_mixed_success_failure_actions() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)), // success
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", true)),  // failed
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)), // success
                buck_event(&t, build_end(false)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            let target_completed = bep.iter().find(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetCompleted(_)),
            )).expect("TargetCompleted should be emitted");

            match target_completed.payload.as_ref().unwrap() {
                build_event_stream::build_event::Payload::Completed(c) => {
                    assert!(!c.success, "TargetCompleted.success should be false when any action failed");
                }
                other => panic!("expected Completed payload, got {:?}", other),
            }
        }

        /// Test that targets with no actions (e.g. cache hits, header-only libraries)
        /// still get TargetCompleted events with success=true.
        #[tokio::test]
        async fn test_build_with_only_cached_targets() {
            let t = TraceId::new();
            // Two targets analyzed but NO ActionExecutionEnd for either
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//lib/..."])),
                buck_event(&t, analysis_start("root//lib:header_only", "cfg//linux-x86_64", "cxx_library")),
                buck_event(&t, analysis_start("root//lib:cached", "cfg//linux-x86_64", "cxx_library")),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            let target_completions: Vec<_> = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetCompleted(_)),
            )).collect();

            assert_eq!(target_completions.len(), 2, "both cached targets should have TargetCompleted");
            for tc in &target_completions {
                match tc.payload.as_ref().unwrap() {
                    build_event_stream::build_event::Payload::Completed(c) => {
                        assert!(c.success, "cached/header-only targets should be successful");
                        assert!(c.output_group.is_empty(), "no output groups for cached targets");
                    }
                    other => panic!("expected Completed payload, got {:?}", other),
                }
            }
        }

        /// Stress test: 10 targets, multiple patterns, many actions.
        #[tokio::test]
        async fn test_dag_large_build() {
            let t = TraceId::new();
            let mut inputs = vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//pkg/...", "root//lib/..."])),
            ];
            // 10 targets with analysis
            for i in 0..10 {
                inputs.push(buck_event(&t, analysis_start(
                    &format!("root//pkg:target{}", i),
                    "cfg//linux-x86_64",
                    "rust_binary",
                )));
            }
            // Multiple actions per target
            for i in 0..10 {
                for _ in 0..3 {
                    inputs.push(buck_event(&t, action_execution_end(
                        &format!("root//pkg:target{}", i),
                        "cfg//linux-x86_64",
                        false,
                    )));
                }
            }
            inputs.push(buck_event(&t, build_end(true)));

            let events = collect_events(inputs).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);

            // Verify counts
            let target_configured_count = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetConfigured(_)),
            )).count();
            let target_completed_count = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetCompleted(_)),
            )).count();
            let action_completed_count = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::ActionCompleted(_)),
            )).count();

            assert_eq!(target_configured_count, 10, "should have 10 TargetConfigured");
            assert_eq!(target_completed_count, 10, "should have 10 TargetCompleted");
            assert_eq!(action_completed_count, 30, "should have 30 ActionCompleted (3 per target)");
        }

        /// Test that events arriving in interleaved order still produce a valid DAG.
        /// Analysis for target B starts before target A finishes its actions.
        #[tokio::test]
        async fn test_interleaved_events() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar", "root//foo:baz"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                // Analysis for baz starts before bar's actions complete
                buck_event(&t, analysis_start("root//foo:baz", "cfg//linux-x86_64", "rust_library")),
                buck_event(&t, action_execution_end("root//foo:baz", "cfg//linux-x86_64", false)),
                // bar's action comes after baz's
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]).await;
            assert_valid_bep_dag(&events);

            let bep = extract_bep_events(&events);
            let target_completed_count = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetCompleted(_)),
            )).count();
            assert_eq!(target_completed_count, 2, "both targets should complete despite interleaving");
        }

        // ================================================================
        // Priority 3: Missing BEP event types
        // ================================================================

        /// Test that test commands (buck2 test) produce full BEP events including
        /// lifecycle events, TestResult for each test case, and TestSummary.
        #[tokio::test]
        async fn test_test_command_produces_events() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, test_command_start()),
                buck_event(&t, analysis_start("root//tests:my_test", "cfg//linux-x86_64", "rust_test")),
                buck_event(&t, action_execution_end("root//tests:my_test", "cfg//linux-x86_64", false)),
                buck_event(&t, test_result("test_foo_passes", "root//tests:my_test", 0)), // PASS
                buck_event(&t, test_result("test_bar_fails", "root//tests:my_test", 1)), // FAIL
                buck_event(&t, test_command_end(false)),
            ]).await;

            // Test commands should produce lifecycle events just like build commands
            let has_lifecycle = events.iter().any(|e| matches!(e, BesTransportEvent::Lifecycle(_)));
            assert!(has_lifecycle, "test commands should produce lifecycle events");

            let bep = extract_bep_events(&events);

            // Should have BuildStarted and BuildFinished
            let has_started = bep.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::Started(_)),
            ));
            assert!(has_started, "test commands should emit BuildStarted");

            // Should have BEP TestResult events for each test case
            let test_results: Vec<_> = bep.iter().filter(|e| matches!(
                e.payload.as_ref(),
                Some(build_event_stream::build_event::Payload::TestResult(_)),
            )).collect();
            assert_eq!(
                test_results.len(), 2,
                "should emit one BEP TestResult per buck2 TestResult instant event",
            );

            // Should have a TestSummary aggregating results
            let has_test_summary = bep.iter().any(|e| matches!(
                e.payload.as_ref(),
                Some(build_event_stream::build_event::Payload::TestSummary(_)),
            ));
            assert!(has_test_summary, "should emit BEP TestSummary for the test target");

            // DAG should be valid
            assert_valid_bep_dag(&events);
        }

        /// Test that a build that is interrupted (no CommandEnd) still produces
        /// a partial event stream. Currently the converter breaks on CommandEnd,
        /// so an interrupted build will yield events up to the interruption point
        /// but no BuildFinished or lifecycle close events.
        #[tokio::test]
        async fn test_aborted_build() {
            let t = TraceId::new();
            // Simulate an interrupted build: start, some analysis, then stream ends
            // without CommandEnd (e.g. ctrl-c or daemon crash)
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                // Stream ends here — no build_end()
            ]).await;

            // We should at least get the initial lifecycle and BuildStarted events
            let has_started = events.iter().any(|e| match e {
                BesTransportEvent::Lifecycle(ev) => matches!(
                    ev.event.as_ref(),
                    Some(v1::build_event::Event::BuildEnqueued(_)),
                ),
                _ => false,
            });
            assert!(has_started, "BuildEnqueued should be emitted even for aborted builds");

            let bep = extract_bep_events(&events);
            let has_started_bep = bep.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::Started(_)),
            ));
            assert!(has_started_bep, "BEP Started should be emitted before abort");

            // When a build is interrupted, the converter should emit an Aborted
            // BEP event so BES consumers know the build didn't complete normally.
            let has_aborted = bep.iter().any(|e| matches!(
                e.payload.as_ref(),
                Some(build_event_stream::build_event::Payload::Aborted(_)),
            ));
            assert!(
                has_aborted,
                "aborted builds should emit a BEP Aborted event when the stream ends without CommandEnd",
            );

            // An aborted build should still emit BuildFinished (with failure status)
            // so that BES servers can close the invocation cleanly.
            let has_finished = bep.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::BuildFinished(_)),
            ));
            assert!(
                has_finished,
                "aborted builds should still emit BuildFinished with failure status",
            );
        }

        /// Test that Configuration events are emitted from AnalysisStart data.
        /// BES consumers need Configuration events to understand multi-config builds.
        #[tokio::test]
        async fn test_configuration_event_emitted() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, build_end(true)),
            ]).await;

            let bep = extract_bep_events(&events);

            let has_target_configured = bep.iter().any(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::TargetConfigured(_)),
            ));
            assert!(has_target_configured, "TargetConfigured should be emitted");

            // Configuration event should be emitted with the config ID from AnalysisStart
            let config_events: Vec<_> = bep.iter().filter(|e| matches!(
                e.id.as_ref().and_then(|id| id.id.as_ref()),
                Some(build_event_id::Id::Configuration(_)),
            )).collect();
            assert!(
                !config_events.is_empty(),
                "Configuration event should be emitted from AnalysisStart configuration data",
            );

            // Verify the configuration ID matches what was in AnalysisStart
            match config_events[0].id.as_ref().unwrap().id.as_ref().unwrap() {
                build_event_id::Id::Configuration(c) => {
                    assert_eq!(c.id, "cfg//linux-x86_64", "Configuration ID should match AnalysisStart config");
                }
                _ => unreachable!(),
            }

            assert_valid_bep_dag(&events);
        }

        // ================================================================
        // Priority 4: BES transport correctness
        // ================================================================

        /// Test that sequence numbers implied by event ordering are monotonically
        /// increasing within each transport stream.
        #[tokio::test]
        async fn test_sequence_numbers_monotonic() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]).await;

            // Separate events into lifecycle and stream buckets
            let mut lifecycle_count: i64 = 0;
            let mut stream_count: i64 = 0;
            for event in &events {
                match event {
                    BesTransportEvent::Lifecycle(_) => lifecycle_count += 1,
                    BesTransportEvent::Stream(_) => stream_count += 1,
                }
            }
            // Lifecycle should have: BuildEnqueued, InvocationAttemptStarted,
            // InvocationAttemptFinished, LifecycleBuildFinished = 4
            assert!(lifecycle_count >= 4, "expected at least 4 lifecycle events, got {}", lifecycle_count);
            // Stream should have: Started, UnstructuredCommandLine, Progress, ActionCompleted,
            // TargetCompleted, PatternExpanded, BuildFinished, ComponentStreamFinished
            assert!(stream_count >= 5, "expected at least 5 stream events, got {}", stream_count);

            // Verify Progress events have monotonically increasing opaque_count
            let bep = extract_bep_events(&events);
            let progress_counts: Vec<i32> = bep.iter().filter_map(|e| {
                match e.id.as_ref()?.id.as_ref()? {
                    build_event_id::Id::Progress(p) => Some(p.opaque_count),
                    _ => None,
                }
            }).collect();
            for window in progress_counts.windows(2) {
                assert!(
                    window[1] > window[0],
                    "Progress opaque_count should be monotonically increasing: {} followed by {}",
                    window[0], window[1],
                );
            }
        }

        /// Test that lifecycle events appear in the correct order.
        #[tokio::test]
        async fn test_event_ordering() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, build_end(true)),
            ]).await;

            let lifecycle_order: Vec<&str> = events.iter().filter_map(|e| {
                match e {
                    BesTransportEvent::Lifecycle(ev) => match ev.event.as_ref()? {
                        v1::build_event::Event::BuildEnqueued(_) => Some("BuildEnqueued"),
                        v1::build_event::Event::InvocationAttemptStarted(_) => Some("InvocationAttemptStarted"),
                        v1::build_event::Event::InvocationAttemptFinished(_) => Some("InvocationAttemptFinished"),
                        v1::build_event::Event::BuildFinished(_) => Some("BuildFinished"),
                        _ => None,
                    },
                    _ => None,
                }
            }).collect();

            assert_eq!(
                lifecycle_order,
                vec!["BuildEnqueued", "InvocationAttemptStarted", "InvocationAttemptFinished", "BuildFinished"],
                "lifecycle events must appear in correct order",
            );

            // Verify stream events: Started must come before Finished
            let stream_order: Vec<&str> = events.iter().filter_map(|e| {
                match e {
                    BesTransportEvent::Stream(ev) => match ev.event.as_ref()? {
                        v1::build_event::Event::BazelEvent(any) => {
                            let bes = build_event_stream::BuildEvent::decode(any.value.as_slice()).ok()?;
                            match bes.id.as_ref()?.id.as_ref()? {
                                build_event_id::Id::Started(_) => Some("Started"),
                                build_event_id::Id::BuildFinished(_) => Some("BuildFinished"),
                                _ => None,
                            }
                        }
                        v1::build_event::Event::ComponentStreamFinished(_) => Some("ComponentStreamFinished"),
                        _ => None,
                    },
                    _ => None,
                }
            }).collect();

            assert_eq!(
                stream_order,
                vec!["Started", "BuildFinished", "ComponentStreamFinished"],
                "stream events must have Started before BuildFinished before ComponentStreamFinished",
            );
        }

        /// Test that only BuildFinished has last_message=true, and all other BEP events have last_message=false.
        #[tokio::test]
        async fn test_last_message_flag() {
            let t = TraceId::new();
            let events = collect_events(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["root//foo:bar"])),
                buck_event(&t, analysis_start("root//foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("root//foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]).await;

            let bep = extract_bep_events(&events);
            assert!(!bep.is_empty(), "should have BEP events");

            for event in &bep {
                let id = event.id.as_ref().and_then(|id| id.id.as_ref());
                let is_build_finished = matches!(id, Some(build_event_id::Id::BuildFinished(_)));
                if is_build_finished {
                    assert!(
                        event.last_message,
                        "BuildFinished must have last_message=true",
                    );
                } else {
                    assert!(
                        !event.last_message,
                        "Non-BuildFinished event should have last_message=false, but {:?} has last_message=true",
                        id,
                    );
                }
            }
        }
    }
}

pub use fbcode::*;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    config: ScribeConfig,
    bes_config: Option<std::sync::Arc<BesConfig>>,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(fbcode_build)]
    {
        let _ = bes_config;
        Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
    }
    #[cfg(not(fbcode_build))]
    {
        let _ = (fb, config);
        match bes_config {
            Some(c) => {
                Ok(Some(RemoteEventSink::new(c).map_err(|e| buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Environment))?))
            }
            _ => Ok(None),
        }
    }
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: ScribeConfig,
    bes_config: Option<std::sync::Arc<BesConfig>>,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(fb, config, bes_config)
    } else {
        Ok(None)
    }
}

/// Whether or not remote event logging is enabled for this process. It must be explicitly disabled via `disable()`.
static REMOTE_EVENT_SINK_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns whether this process should actually write to remote sink, even if it is fully supported by the platform and
/// binary.
pub fn is_enabled() -> bool {
    REMOTE_EVENT_SINK_ENABLED.load(Ordering::Relaxed)
}

/// Disables remote event logging for this process. Remote event logging must be disabled explicitly on startup, otherwise it is
/// on by default.
pub fn disable() {
    REMOTE_EVENT_SINK_ENABLED.store(false, Ordering::Relaxed);
}
