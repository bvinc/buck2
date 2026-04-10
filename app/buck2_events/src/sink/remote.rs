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

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(events: S) -> impl Stream<Item = BesTransportEvent> {
        let mut target_actions: HashMap<(String, String), Vec<(BuildEventId, bool)>> = HashMap::new();
        // Track configured targets for the BEP event graph: (label, config_full_name, rule_type)
        let mut configured_targets: Vec<(String, String, String)> = Vec::new();
        // Track parsed target patterns from ParsedTargetPatterns instant event
        let mut parsed_patterns: Vec<String> = Vec::new();
        // Progress event chain counter. BuildStarted declares Progress(0) as child.
        // Each Progress(N) declares Progress(N+1) as child, forming a chain.
        // The final Progress at CommandEnd adopts PatternExpanded events.
        let mut progress_count: i32 = 0;
        stream! {
            for await event in events {
                match event.data() {
                    buck2_data::buck_event::Data::SpanStart(start) => {
                        match start.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_start_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_start::Data::Build(BuildCommandStart {})) => {
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
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                                // Extract label and configuration from ConfiguredTargetLabel
                                let (label, config, rule) = match analysis.target.as_ref() {
                                    Some(buck2_data::analysis_start::Target::StandardTarget(ct)) => {
                                        let label = ct.label.as_ref().map(|l| format!("{}:{}", l.package, l.name));
                                        let config = ct.configuration.as_ref().map(|c| c.full_name.clone());
                                        (label, config, analysis.rule.clone())
                                    }
                                    _ => (None, None, String::new()),
                                };
                                if let (Some(label), Some(config)) = (label, config) {
                                    configured_targets.push((label.clone(), config.clone(), rule.clone()));
                                    // BEP: TargetConfigured — declares TargetCompleted as child
                                    let bes_event = build_event_stream::BuildEvent {
                                        id: Some(BuildEventId { id: Some(build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                            label: label.clone(),
                                            aspect: "".to_owned(),
                                        })) }),
                                        children: vec![
                                            BuildEventId { id: Some(build_event_id::Id::TargetCompleted(build_event_id::TargetCompletedId {
                                                label: label.clone(),
                                                configuration: Some(build_event_id::ConfigurationId { id: config }),
                                                aspect: "".to_owned(),
                                            }))},
                                        ],
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
                                    // PatternExpanded is emitted at CommandEnd via Progress(0)
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
                                    Some(buck2_data::command_end::Data::Build(_build)) => {
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

                                        // Flush the target completed map.
                                        for ((label, config), actions) in target_actions.into_iter() {
                                            let success = actions.iter().all(|(_, success)| *success);
                                            let children: Vec<_> = actions.into_iter().map(|(id, _)| id).collect();
                                            let bes_event = build_event_stream::BuildEvent {
                                                id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetCompleted(build_event_id::TargetCompletedId {
                                                    label: label,
                                                    configuration: Some(build_event_id::ConfigurationId { id: config }),
                                                    aspect: "".to_owned(),
                                                })) }),
                                                children: children,
                                                last_message: false,
                                                payload: Some(build_event_stream::build_event::Payload::Completed(build_event_stream::TargetComplete {
                                                    success: success,
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
                                                // TODO: convert Buck2 ErrorReport
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
                                }.map(|label| format!("{}:{}", label.package, label.name));
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
                                    Some(_) => vec![], // TODO: handle remote, worker, and other commands
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
                                //let wall_time = last_command_details.and_then(|details| details.metadata.as_ref().and_then(|metadata| metadata.wall_time.clone()));
                                //let end_time = ...; // TODO: add start_time and wall_time
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
                                        end_time: None,
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
                            _ => {}
                        }
                    },
                    buck2_data::buck_event::Data::Record(_record) => {
                        //println!("REC   {:?}", record);
                    },
                }
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

        #[tokio::test]
        async fn test_dag_build_with_targets() {
            let t = TraceId::new();
            let stream = tokio_stream::iter(vec![
                buck_event(&t, build_start()),
                buck_event(&t, parsed_target_patterns(&["//foo/..."])),
                buck_event(&t, analysis_start("foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, analysis_start("foo:baz", "cfg//linux-x86_64", "rust_library")),
                buck_event(&t, action_execution_end("foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("foo:baz", "cfg//linux-x86_64", false)),
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
                buck_event(&t, analysis_start("foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, action_execution_end("foo:bar", "cfg//linux-x86_64", false)),
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
                buck_event(&t, parsed_target_patterns(&["//foo:bar", "//foo:baz"])),
                buck_event(&t, analysis_start("foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, analysis_start("foo:baz", "cfg//linux-x86_64", "rust_library")),
                buck_event(&t, action_execution_end("foo:bar", "cfg//linux-x86_64", false)),
                buck_event(&t, action_execution_end("foo:baz", "cfg//linux-x86_64", false)),
                buck_event(&t, build_end(true)),
            ]);
            let events: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_valid_bep_dag(&events);
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
                buck_event(&t, parsed_target_patterns(&["//foo:bar"])),
                buck_event(&t, console_message("Analyzing target //foo:bar")),
                buck_event(&t, analysis_start("foo:bar", "cfg//linux-x86_64", "rust_binary")),
                buck_event(&t, console_message("Building //foo:bar")),
                buck_event(&t, action_execution_end("foo:bar", "cfg//linux-x86_64", false)),
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
            assert_eq!(progress_events[0], (0, "Analyzing target //foo:bar".to_owned()));
            assert_eq!(progress_events[1], (1, "Building //foo:bar".to_owned()));
            assert_eq!(progress_events[2].0, 2);
            assert!(progress_events[2].1.is_empty());
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
