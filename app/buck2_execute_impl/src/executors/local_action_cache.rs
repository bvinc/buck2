/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

//! Local action cache: persists action results in SQLite so that local builds
//! can skip re-execution of unchanged actions across daemon restarts.

use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use buck2_common::file_ops::metadata::FileDigestConfig;
use buck2_core::content_hash::ContentBasedPathHash;
use buck2_core::fs::artifact_path_resolver::ArtifactFs;
use buck2_core::fs::project::ProjectRoot;
use buck2_execute::artifact_value::ArtifactValue;
use buck2_execute::digest_config::DigestConfig;
use buck2_execute::directory::extract_artifact_value;
use buck2_execute::directory::insert_entry;
use buck2_execute::entry::build_entry_from_disk;
use buck2_execute::execute::action_digest_and_blobs::ActionDigestAndBlobs;
use buck2_execute::execute::blocking::BlockingExecutor;
use buck2_execute::execute::cache_uploader::CacheUploadInfo;
use buck2_execute::execute::cache_uploader::CacheUploadResult;
use buck2_execute::execute::cache_uploader::IntoRemoteDepFile;
use buck2_execute::execute::cache_uploader::UploadCache;
use buck2_execute::execute::inputs_directory::inputs_directory;
use buck2_execute::execute::kind::CommandExecutionKind;
use buck2_execute::execute::manager::CommandExecutionManager;
use buck2_execute::execute::output::CommandStdStreams;
use buck2_execute::execute::prepared::PreparedCommand;
use buck2_execute::execute::prepared::PreparedCommandOptionalExecutor;
use buck2_execute::execute::request::CommandExecutionOutput;
use buck2_execute::execute::result::CommandExecutionMetadata;
use buck2_execute::execute::result::CommandExecutionResult;
use buck2_execute::materialize::materializer::DeclareArtifactPayload;
use buck2_execute::materialize::materializer::DeclareMatchOutcome;
use buck2_execute::materialize::materializer::Materializer;
use buck2_hash::BuckIndexMap;
use buck2_util::time_span::TimeSpan;
use dice_futures::cancellation::CancellationContext;
use dupe::Dupe;
use remote_execution::TActionResult2;

use crate::sqlite::tables::local_action_cache_table::LocalActionCacheSqliteTable;

/// Writes successful local execution results to the local SQLite action cache.
pub struct LocalActionCacheUploader {
    pub artifact_fs: ArtifactFs,
    pub action_cache_table: Arc<LocalActionCacheSqliteTable>,
}

#[async_trait]
impl UploadCache for LocalActionCacheUploader {
    async fn upload(
        &self,
        _info: &CacheUploadInfo<'_>,
        execution_result: &CommandExecutionResult,
        _re_result: Option<TActionResult2>,
        _dep_file_bundle: Option<&mut dyn IntoRemoteDepFile>,
        action_digest_and_blobs: &ActionDigestAndBlobs,
    ) -> buck2_error::Result<CacheUploadResult> {
        if execution_result.was_locally_executed() && execution_result.was_success() {
            let digest_str = action_digest_and_blobs.action.to_string();
            record_in_local_action_cache(
                &self.action_cache_table,
                &digest_str,
                execution_result,
                &self.artifact_fs,
            );
        }
        Ok(CacheUploadResult {
            did_cache_upload: false,
            did_dep_file_cache_upload: false,
            dep_file_cache_upload_key: None,
        })
    }
}

/// Checks the local SQLite action cache before executing a command.
/// On a cache hit, re-reads output metadata from disk and returns
/// the result without re-executing the action.
pub struct LocalActionCacheChecker {
    pub artifact_fs: ArtifactFs,
    pub materializer: Arc<dyn Materializer>,
    pub blocking_executor: Arc<dyn BlockingExecutor>,
    pub project_root: ProjectRoot,
    pub action_cache_table: Arc<LocalActionCacheSqliteTable>,
}

#[async_trait]
impl PreparedCommandOptionalExecutor for LocalActionCacheChecker {
    async fn maybe_execute(
        &self,
        command: &PreparedCommand<'_, '_>,
        manager: CommandExecutionManager,
        _cancellations: &CancellationContext,
    ) -> ControlFlow<CommandExecutionResult, CommandExecutionManager> {
        let action_digest = &command.prepared_action.action_and_blobs.action;
        let digest_str = action_digest.to_string();

        // Look up in the local action cache
        let cached = match self.action_cache_table.lookup(&digest_str) {
            Ok(Some(data)) => data,
            Ok(None) => return ControlFlow::Continue(manager),
            Err(e) => {
                tracing::debug!("Local action cache lookup error: {:#}", e);
                return ControlFlow::Continue(manager);
            }
        };

        // Parse the cached output paths to verify they still exist
        let cached_paths: Vec<&str> = cached.lines().collect();
        if cached_paths.is_empty() {
            return ControlFlow::Continue(manager);
        }

        let request = command.request;
        let digest_config = command.digest_config;

        // Collect outputs (converting refs to owned)
        let outputs: Vec<CommandExecutionOutput> =
            request.outputs().map(|o| o.cloned()).collect();

        // Try to re-read outputs from disk and verify they match
        match self
            .verify_and_rebuild_outputs(outputs, digest_config, request.inputs())
            .await
        {
            Ok(outputs) => {
                // Declare the artifacts as existing with the materializer
                let to_declare: Vec<DeclareArtifactPayload> = outputs
                    .iter()
                    .filter_map(|(output, value)| {
                        if let CommandExecutionOutput::BuildArtifact { .. } = output {
                            let path = output
                                .as_ref()
                                .resolve(
                                    &self.artifact_fs,
                                    Some(&ContentBasedPathHash::for_output_artifact()),
                                )
                                .ok()?
                                .into_path();
                            Some(DeclareArtifactPayload {
                                path,
                                artifact: value.dupe(),
                                persist_full_directory_structure: false,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();

                // Use declare_match to verify artifacts are still valid on disk
                match self.materializer.declare_match(
                    to_declare
                        .iter()
                        .map(|p| (p.path.clone(), p.artifact.dupe()))
                        .collect(),
                ).await {
                    Ok(DeclareMatchOutcome::Match) => {}
                    Ok(DeclareMatchOutcome::NotMatch) => {
                        tracing::debug!(
                            "Local action cache: outputs changed on disk for {}",
                            digest_str
                        );
                        return ControlFlow::Continue(manager);
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Local action cache: declare_match failed for {}: {:#}",
                            digest_str,
                            e
                        );
                        return ControlFlow::Continue(manager);
                    }
                }

                tracing::info!(
                    "Local action cache hit, skipping execution for action `{}`",
                    action_digest,
                );

                let timing = CommandExecutionMetadata::empty(TimeSpan::empty_now());
                let manager = manager.claim().await;
                let result = manager.success(
                    CommandExecutionKind::LocalActionCache {
                        digest: action_digest.dupe(),
                    },
                    outputs,
                    CommandStdStreams::Local {
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                    },
                    timing,
                );
                ControlFlow::Break(result)
            }
            Err(e) => {
                tracing::debug!(
                    "Local action cache: failed to rebuild outputs for {}: {:#}",
                    digest_str,
                    e
                );
                ControlFlow::Continue(manager)
            }
        }
    }
}

impl LocalActionCacheChecker {
    /// Re-read output files from disk and reconstruct ArtifactValues.
    /// Returns None if any output is missing or can't be read.
    async fn verify_and_rebuild_outputs(
        &self,
        outputs: Vec<CommandExecutionOutput>,
        digest_config: DigestConfig,
        inputs: &[buck2_execute::execute::request::CommandExecutionInput],
    ) -> buck2_error::Result<BuckIndexMap<CommandExecutionOutput, ArtifactValue>> {
        let mut builder = inputs_directory(inputs, digest_config, &self.artifact_fs)?;

        let mut paths = Vec::new();
        for output in &outputs {
            let path = output
                .as_ref()
                .resolve(
                    &self.artifact_fs,
                    Some(&ContentBasedPathHash::for_output_artifact()),
                )?
                .into_path();
            let abspath = self.project_root.root().join(&path);
            let (entry, _hashing_info) = build_entry_from_disk(
                abspath,
                FileDigestConfig::build(digest_config.cas_digest_config()),
                self.blocking_executor.as_ref(),
                self.artifact_fs.fs().root(),
            )
            .await?;

            if let Some(entry) = entry {
                insert_entry(&mut builder, path.clone(), entry)?;
                paths.push(path);
            } else {
                // Output missing from disk - cache miss
                return Err(buck2_error::buck2_error!(
                    buck2_error::ErrorTag::Input,
                    "Output missing from disk"
                ));
            }
        }

        let mut mapped_outputs = BuckIndexMap::with_capacity(outputs.len());
        for (output, output_path) in outputs.into_iter().zip(paths) {
            let value = extract_artifact_value(&builder, &output_path, digest_config)?;
            if let Some(value) = value {
                mapped_outputs.insert(output, value);
            }
        }

        Ok(mapped_outputs)
    }
}

/// Record a successful local execution in the action cache.
pub fn record_in_local_action_cache(
    action_cache_table: &LocalActionCacheSqliteTable,
    action_digest: &str,
    result: &CommandExecutionResult,
    artifact_fs: &ArtifactFs,
) {
    if !result.was_locally_executed() {
        return;
    }

    // Store the output paths as newline-delimited text
    let output_paths: Vec<String> = result
        .outputs
        .keys()
        .filter_map(|output| {
            output
                .as_ref()
                .resolve(artifact_fs, Some(&ContentBasedPathHash::for_output_artifact()))
                .ok()
                .map(|resolved| resolved.into_path().to_string())
        })
        .collect();

    let output_data = output_paths.join("\n");

    if let Err(e) = action_cache_table.insert(action_digest, &output_data) {
        tracing::debug!("Failed to write to local action cache: {:#}", e);
    }
}
