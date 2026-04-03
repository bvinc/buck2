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

#[cfg(fbcode_build)]
mod fbcode {
    pub use scribe_client::ScribeConfig;

    pub use crate::sink::scribe::RemoteEventSink;
    pub(crate) use crate::sink::scribe::scribe_category;
}

#[cfg(not(fbcode_build))]
mod fbcode {
    use std::collections::HashMap;
    use std::env::VarError;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use allocative::Allocative;
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
    use bazel_event_publisher_proto::google::devtools::build::v1::StreamId;

    use prost;
    use prost::Message;
    use prost_types;

    use regex::Regex;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    pub struct RemoteEventSink {
        _handler: JoinHandle<()>,
        send: UnboundedSender<Vec<BuckEvent>>,
    }

    // TODO[AH] re-use definitions from REOSS crate.
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct HttpHeader {
        pub key: String,
        pub value: String,
    }

    impl FromStr for HttpHeader {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut iter = s.split(':');
            match (iter.next(), iter.next(), iter.next()) {
                (Some(key), Some(value), None) => Ok(Self {
                    key: key.trim().to_owned(),
                    value: value.trim().to_owned(),
                }),
                _ => Err(anyhow::anyhow!(
                    "Invalid header (expect exactly one `:`): `{}`",
                    s
                )),
            }
        }
    }

    /// Replace occurrences of $FOO in a string with the value of the env var $FOO.
    fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
        substitute_env_vars_impl(s, |v| std::env::var(v))
    }

    fn substitute_env_vars_impl(
        s: &str,
        getter: impl Fn(&str) -> Result<String, VarError>,
    ) -> anyhow::Result<String> {
        static ENV_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

        let mut out = String::with_capacity(s.len());
        let mut last_idx: usize = 0;

        for mat in ENV_REGEX.find_iter(s) {
            out.push_str(&s[last_idx..mat.start()]);
            let var = &mat.as_str()[1..];
            let val = getter(var).with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
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
                    // This means we can't have `$` in a header key or value, which isn't great. On the
                    // flip side, env vars are good for things like credentials, which those headers
                    // are likely to contain. In time, we should allow escaping.
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

    async fn connect_build_event_server() -> anyhow::Result<PublishBuildEventClient<GrpcService>> {
        let uri = std::env::var("BES_URI")?.parse()?;
        let mut channel = Channel::builder(uri);
        let tls_config = ClientTlsConfig::new();
        {
            let tls_setting = std::env::var("BES_TLS").unwrap_or("0".to_owned());
            match tls_setting.as_str() {
                "1" | "true" => {
                    channel = channel.tls_config(tls_config)?;
                },
                _ => {},
            }
        }
        // TODO: parse PEM
        let endpoint = channel
            .connect()
            .await
            .context("connecting to Bazel event stream gRPC server")?;
        let mut headers = vec![];
        for hdr in std::env::var("BES_HEADERS").unwrap_or("".to_owned()).split(",") {
            let hdr = hdr.trim();
            if !hdr.is_empty() {
                headers.push(HttpHeader::from_str(hdr)?);
            }
        };
        let interceptor = InjectHeadersInterceptor::new(&headers)?;
        let client = PublishBuildEventClient::with_interceptor(endpoint, interceptor);
        Ok(client)
    }

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(events: S) -> impl Stream<Item = v1::BuildEvent> {
        let mut target_actions: HashMap<(String, String), Vec<(BuildEventId, bool)>> = HashMap::new();
        stream! {
            for await event in events {
                //println!("EVENT {:?} {:?}", event.event.trace_id, event);
                match event.data() {
                    buck2_data::buck_event::Data::SpanStart(start) => {
                        //println!("START {:?}", start);
                        match start.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_start_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_start::Data::Build(BuildCommandStart {})) => {
                                        // Lifecycle: BuildEnqueued
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::BuildEnqueued(
                                                v1::build_event::BuildEnqueued { details: None },
                                            )),
                                        };
                                        // Lifecycle: InvocationAttemptStarted
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::InvocationAttemptStarted(
                                                v1::build_event::InvocationAttemptStarted {
                                                    attempt_number: 1,
                                                    details: None,
                                                },
                                            )),
                                        };
                                        // BEP: BuildStarted
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Started(build_event_stream::build_event_id::BuildStartedId {})) }),
                                            children: vec![],
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
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
                                    },
                                    Some(_) => {},
                                }
                            },
                            Some(buck2_data::span_start_event::Data::Analysis(analysis)) => {
                                let label = match analysis.target.as_ref() {
                                    None => None,
                                    Some(buck2_data::analysis_start::Target::StandardTarget(label)) =>
                                        label.label.as_ref().map(|label| format!("{}:{}", label.package, label.name)),
                                    Some(buck2_data::analysis_start::Target::AnonTarget(_anon)) => None, // TODO
                                    Some(buck2_data::analysis_start::Target::DynamicLambda(_owner)) => None, // TODO
                                };
                                match label {
                                    None => {},
                                    Some(label) => {
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetConfigured(build_event_id::TargetConfiguredId {
                                                label: label.clone(),
                                                aspect: "".to_owned(),
                                            })) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Configured(bazel_event_publisher_proto::build_event_stream::TargetConfigured {
                                                target_kind: "UNKNOWN".to_owned(),
                                                test_size: 0,
                                                tag: vec![],
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };

                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Pattern(build_event_id::PatternExpandedId {
                                                pattern: vec![label.clone()],
                                            })) }),
                                            children: vec![
                                                build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::TargetConfigured(bazel_event_publisher_proto::build_event_stream::build_event_id::TargetConfiguredId {
                                                    label: label,
                                                    aspect: "".to_owned(),
                                                }))},
                                            ],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Expanded(bazel_event_publisher_proto::build_event_stream::PatternExpanded {
                                                test_suite_expansions: vec![],
                                            })),
                                        };
                                        let bazel_event = v1::build_event::Event::BazelEvent(prost_types::Any {
                                            type_url: "type.googleapis.com/build_event_stream.BuildEvent".to_owned(),
                                            value: bes_event.encode_to_vec(),
                                        });
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
                                    },
                                }
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::SpanEnd(end) => {
                        //println!("END   {:?}", end);
                        match end.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_end_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_end::Data::Build(_build)) => {
                                        // flush the target completed map.
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
                                            yield v1::BuildEvent {
                                                event_time: Some(event.timestamp().into()),
                                                event: Some(bazel_event),
                                            };
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
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(bazel_event),
                                        };
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
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::InvocationAttemptFinished(
                                                v1::build_event::InvocationAttemptFinished {
                                                    invocation_status: Some(status.clone()),
                                                    details: None,
                                                },
                                            )),
                                        };
                                        // Lifecycle: BuildFinished
                                        yield v1::BuildEvent {
                                            event_time: Some(event.timestamp().into()),
                                            event: Some(v1::build_event::Event::BuildFinished(
                                                v1::build_event::BuildFinished {
                                                    status: Some(status),
                                                    details: None,
                                                },
                                            )),
                                        };
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
                                yield v1::BuildEvent {
                                    event_time: Some(event.timestamp().into()),
                                    event: Some(bazel_event),
                                };
                            },
                            Some(_) => {},
                        }
                    },
                    buck2_data::buck_event::Data::Instant(_instant) => {
                        //println!("INST  {:?}", instant);
                    },
                    buck2_data::buck_event::Data::Record(_record) => {
                        //println!("REC   {:?}", record);
                    },
                }
            }
        }
    }

    fn stream_build_tool_events<S: Stream<Item = v1::BuildEvent>>(trace_id: String, events: S) -> impl Stream<Item = PublishBuildToolEventStreamRequest> {
        stream::iter(1..)
            .zip(events)
            .map(move |(sequence_number, event)| {
                PublishBuildToolEventStreamRequest {
                    check_preceding_lifecycle_events_present: false,
                    notification_keywords: vec![],
                    ordered_build_event: Some(OrderedBuildEvent {
                        stream_id: Some(StreamId {
                            build_id: trace_id.clone(),
                            invocation_id: trace_id.clone(),
                            component: 0,
                        }),
                        sequence_number,
                        event: Some(event),
                    }),
                    project_id: "12341234".to_owned(), // TODO: needed
                }
            })
    }

    async fn event_sink_loop(recv: UnboundedReceiver<Vec<BuckEvent>>) -> anyhow::Result<()> {
        let mut handlers: HashMap<String, (UnboundedSender<BuckEvent>, tokio::task::JoinHandle<anyhow::Result<()>>)> = HashMap::new();
        let client = connect_build_event_server().await?;
        let mut recv = UnboundedReceiverStream::new(recv)
            .flat_map(|v|stream::iter(v));
        let result_uri = std::env::var("BES_RESULT").ok();
        while let Some(event) = recv.next().await {
            //let dbg_trace_id = event.event.trace_id.clone();
            //println!("event_sink_loop event {:?}", &dbg_trace_id);
            if let Some((send, _)) = handlers.get(&event.event.trace_id) {
                //println!("event_sink_loop redirect {:?}", &dbg_trace_id);
                send.send(event).unwrap_or_else(|e| println!("build event send failed {:?}", e));
            } else {
                //println!("event_sink_loop new handler {:?}", event.event.trace_id);
                let (send, recv) = mpsc::unbounded_channel::<BuckEvent>();
                let mut client = client.clone();
                let result_uri = result_uri.clone();
                //let dbg_trace_id = dbg_trace_id.clone();
                let trace_id = event.event.trace_id.clone();
                let handler = tokio::spawn(async move {
                    let recv = UnboundedReceiverStream::new(recv);
                    let request = Request::new(stream_build_tool_events(trace_id.clone(), buck_to_bazel_events(recv)));
                    if let Some(result_uri) = result_uri.as_ref() {
                        println!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    //println!("BES request {:?}", &dbg_trace_id);
                    let response = client.publish_build_tool_event_stream(request).await?;
                    //println!("BES response {:?}", &dbg_trace_id);
                    let mut inbound = response.into_inner();
                    while let Some(_ack) = inbound.message().await? {
                        // TODO: Handle ACKs properly and add retry.
                        //println!("ACK  {:?}", ack);
                    }
                    if let Some(result_uri) = result_uri.as_ref() {
                        println!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    Ok(())
                });
                handlers.insert(event.event.trace_id.to_owned(), (send, handler));
            }
        }
        //println!("event_sink_loop recv CLOSED");
        // TODO: handle closure and retry.
        // close send handles and await all handlers.
        let handlers: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> = handlers.into_values().map(|(_, handler)|handler).collect();
        // TODO: handle retry.
        try_join_all(handlers).await?.into_iter().collect::<anyhow::Result<Vec<()>>>()?;
        Ok(())
    }

    impl RemoteEventSink {
        pub fn new() -> anyhow::Result<Self> {
            let (send, recv) = mpsc::unbounded_channel::<Vec<BuckEvent>>();
            let handler = std::thread::Builder::new()
                .name("buck-event-producer".to_owned())
                .spawn({
                    move || {
                        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                        runtime.block_on(event_sink_loop(recv)).unwrap();
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

        /// Describes what we expect a BES output event to look like.
        #[derive(Debug)]
        enum ExpectedBesEvent {
            // Lifecycle events (v1::BuildEvent variants)
            BuildEnqueued,
            InvocationAttemptStarted,
            InvocationAttemptFinished,
            LifecycleBuildFinished,
            // BEP events (wrapped in BazelEvent)
            Started,
            Finished { success: bool },
            TargetConfigured { label: String },
            PatternExpanded,
            ActionCompleted { label: String },
            TargetCompleted { label: String },
        }

        /// Assert that a BES event matches an expectation.
        fn assert_bes_matches(actual: &v1::BuildEvent, expected: &ExpectedBesEvent) {
            let event = actual.event.as_ref().unwrap();
            match expected {
                // Lifecycle events: check the v1::BuildEvent variant directly
                ExpectedBesEvent::BuildEnqueued => {
                    assert!(
                        matches!(event, v1::build_event::Event::BuildEnqueued(_)),
                        "expected BuildEnqueued, got {:?}", event,
                    );
                }
                ExpectedBesEvent::InvocationAttemptStarted => {
                    assert!(
                        matches!(event, v1::build_event::Event::InvocationAttemptStarted(_)),
                        "expected InvocationAttemptStarted, got {:?}", event,
                    );
                }
                ExpectedBesEvent::InvocationAttemptFinished => {
                    assert!(
                        matches!(event, v1::build_event::Event::InvocationAttemptFinished(_)),
                        "expected InvocationAttemptFinished, got {:?}", event,
                    );
                }
                ExpectedBesEvent::LifecycleBuildFinished => {
                    assert!(
                        matches!(event, v1::build_event::Event::BuildFinished(_)),
                        "expected lifecycle BuildFinished, got {:?}", event,
                    );
                }
                // BEP events: decode the inner build_event_stream::BuildEvent
                _ => {
                    let any = match event {
                        v1::build_event::Event::BazelEvent(any) => any,
                        other => panic!("expected BazelEvent, got {:?}", other),
                    };
                    let bes = build_event_stream::BuildEvent::decode(any.value.as_slice()).unwrap();
                    let id = bes.id.as_ref().and_then(|id| id.id.as_ref());
                    match expected {
                        ExpectedBesEvent::Started => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::Started(_))),
                                "expected Started, got {:?}", id,
                            );
                        }
                        ExpectedBesEvent::Finished { success } => {
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
                        ExpectedBesEvent::TargetConfigured { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::TargetConfigured(tc)) => {
                                    assert_eq!(&tc.label, label, "TargetConfigured label mismatch");
                                }
                                _ => panic!("expected TargetConfigured, got {:?}", id),
                            }
                        }
                        ExpectedBesEvent::PatternExpanded => {
                            assert!(
                                matches!(id, Some(build_event_stream::build_event_id::Id::Pattern(_))),
                                "expected Pattern, got {:?}", id,
                            );
                        }
                        ExpectedBesEvent::ActionCompleted { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::ActionCompleted(ac)) => {
                                    assert_eq!(&ac.label, label, "ActionCompleted label mismatch");
                                }
                                _ => panic!("expected ActionCompleted, got {:?}", id),
                            }
                        }
                        ExpectedBesEvent::TargetCompleted { label } => {
                            match id {
                                Some(build_event_stream::build_event_id::Id::TargetCompleted(tc)) => {
                                    assert_eq!(&tc.label, label, "TargetCompleted label mismatch");
                                }
                                _ => panic!("expected TargetCompleted, got {:?}", id),
                            }
                        }
                        // Lifecycle variants already handled above
                        _ => unreachable!(),
                    }
                }
            }
        }

        /// Run a list of input BuckEvents through the converter and assert the
        /// output matches the expected BES events.
        async fn check(inputs: Vec<BuckEvent>, expected: Vec<ExpectedBesEvent>) {
            let stream = tokio_stream::iter(inputs);
            let actual: Vec<_> = buck_to_bazel_events(stream).collect().await;
            assert_eq!(
                actual.len(),
                expected.len(),
                "expected {} BES events, got {}",
                expected.len(),
                actual.len(),
            );
            for (i, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
                assert_bes_matches(actual, expected);
                if let Err(_) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    assert_bes_matches(actual, expected);
                })) {
                    panic!("BES event {} mismatch: expected {:?}", i, expected);
                }
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
                    ExpectedBesEvent::BuildEnqueued,
                    ExpectedBesEvent::InvocationAttemptStarted,
                    ExpectedBesEvent::Started,
                    ExpectedBesEvent::Finished { success: true },
                    ExpectedBesEvent::InvocationAttemptFinished,
                    ExpectedBesEvent::LifecycleBuildFinished,
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
                    ExpectedBesEvent::BuildEnqueued,
                    ExpectedBesEvent::InvocationAttemptStarted,
                    ExpectedBesEvent::Started,
                    ExpectedBesEvent::Finished { success: false },
                    ExpectedBesEvent::InvocationAttemptFinished,
                    ExpectedBesEvent::LifecycleBuildFinished,
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
    }
}

pub use fbcode::*;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(fbcode_build)]
    {
        Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
    }
    #[cfg(not(fbcode_build))]
    {
        let _ = (fb, config);
        match std::env::var("BES_URI") {
          Ok(_) => Ok(Some(RemoteEventSink::new().map_err(|e| buck2_error::conversion::from_any_with_tag(e, buck2_error::ErrorTag::Environment))?)),
          _ => Ok(None),
        }
    }
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(fb, config)
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
