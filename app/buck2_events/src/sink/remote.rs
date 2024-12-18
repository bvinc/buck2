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
    use futures::stream;

    use futures::Stream;
    use futures::StreamExt;
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

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    pub struct RemoteEventSink {
        _handler: JoinHandle<()>,
        send: UnboundedSender<Vec<BuckEvent>>,
    }

    async fn connect_build_event_server() -> anyhow::Result<PublishBuildEventClient<Channel>> {
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
        // TODO: handle API token
        channel
            .connect()
            .await
            .context("connecting to Bazel event stream gRPC server")?;
        let client = PublishBuildEventClient::connect(channel)
            .await
            .context("creating Bazel event stream gRPC client")?;
        Ok(client)
    }

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(events: S) -> impl Stream<Item = v1::BuildEvent> {
        let mut target_actions: HashMap<(String, String), Vec<(BuildEventId, bool)>> = HashMap::new();
        stream! {
            for await event in events {
                println!("EVENT {:?} {:?}", event.event.trace_id, event);
                match event.data() {
                    buck2_data::buck_event::Data::SpanStart(start) => {
                        println!("START {:?}", start);
                        match start.data.as_ref() {
                            None => {},
                            Some(buck2_data::span_start_event::Data::Command(command)) => {
                                match command.data.as_ref() {
                                    None => {},
                                    Some(buck2_data::command_start::Data::Build(BuildCommandStart {})) => {
                                        let bes_event = build_event_stream::BuildEvent {
                                            id: Some(build_event_stream::BuildEventId { id: Some(build_event_stream::build_event_id::Id::Started(build_event_stream::build_event_id::BuildStartedId {})) }),
                                            children: vec![],
                                            last_message: false,
                                            payload: Some(build_event_stream::build_event::Payload::Started(build_event_stream::BuildStarted {
                                                uuid: event.event.trace_id.clone(),
                                                start_time_millis: 0,
                                                start_time: Some(event.timestamp().into()),
                                                build_tool_version: "BUCK2".to_owned(),
                                                options_description: "UNKNOWN".to_owned(),
                                                command: "build".to_owned(),
                                                working_directory: "UNKNOWN".to_owned(),
                                                workspace_directory: "UNKNOWN".to_owned(),
                                                server_pid: std::process::id() as i64,
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
                        println!("END   {:?}", end);
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
                                                    target_kind: "".to_owned(),
                                                    test_size: 0,
                                                    output_group: vec![],
                                                    important_output: vec![],
                                                    directory_output: vec![],
                                                    tag: vec![],
                                                    test_timeout_seconds: 0,
                                                    test_timeout: None,
                                                    failure_detail: None,
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
                                                overall_success: command.is_success,
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
                                                finish_time_millis: 0,
                                                finish_time: Some(event.timestamp().into()),
                                                anomaly_report: None,
                                                // TODO: convert Buck2 ErrorReport
                                                failure_detail: None,
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
                                    label: label.clone().unwrap_or("UNKOWN".to_owned()),
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
                                let stdout = last_command_details.map(|details| details.stdout.clone());
                                let stderr = last_command_details.map(|details| details.stderr.clone());
                                let stdout_file = stdout.map(|stdout| bazel_event_publisher_proto::build_event_stream::File {
                                    path_prefix: vec![],
                                    name: "stdout".to_owned(),
                                    digest: "".to_owned(),
                                    length: stdout.len() as i64,
                                    file: Some(bazel_event_publisher_proto::build_event_stream::file::File::Contents(stdout.into())),
                                });
                                let stderr_file = stderr.clone().map(|stderr| bazel_event_publisher_proto::build_event_stream::File {
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
                                        label: "".to_owned(),
                                        configuration: None,
                                        primary_output: None,
                                        command_line: command_line,
                                        action_metadata_logs: vec![],
                                        failure_detail: failure_detail,
                                        start_time: start_time, // TODO: should we deduct queue time?
                                        end_time: None,
                                        strategy_details: vec![],
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
                    buck2_data::buck_event::Data::Instant(instant) => {
                        println!("INST  {:?}", instant);
                    },
                    buck2_data::buck_event::Data::Record(record) => {
                        println!("REC   {:?}", record);
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
        while let Some(event) = recv.next().await {
            let dbg_trace_id = event.event.trace_id.clone();
            println!("event_sink_loop event {:?}", &dbg_trace_id);
            if let Some((send, _)) = handlers.get(&event.event.trace_id) {
                println!("event_sink_loop redirect {:?}", &dbg_trace_id);
                send.send(event).unwrap_or_else(|e| println!("build event send failed {:?}", e));
            } else {
                println!("event_sink_loop new handler {:?}", event.event.trace_id);
                let (send, recv) = mpsc::unbounded_channel::<BuckEvent>();
                let mut client = client.clone();
                let dbg_trace_id = dbg_trace_id.clone();
                let trace_id = event.event.trace_id.clone();
                let handler = tokio::spawn(async move {
                    let recv = UnboundedReceiverStream::new(recv);
                    let request = Request::new(stream_build_tool_events(trace_id, buck_to_bazel_events(recv)));
                    println!("new handler request {:?}", &dbg_trace_id);
                    let response = client.publish_build_tool_event_stream(request).await?;
                    println!("new handler response {:?}", &dbg_trace_id);
                    let mut inbound = response.into_inner();
                    while let Some(ack) = inbound.message().await? {
                        // TODO: Handle ACKs properly and add retry.
                        println!("ACK  {:?}", ack);
                    }
                    Ok(())
                });
                handlers.insert(event.event.trace_id.to_owned(), (send, handler));
            }
        }
        println!("event_sink_loop recv CLOSED");
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
        Ok(Some(RemoteEventSink::new()?))
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
