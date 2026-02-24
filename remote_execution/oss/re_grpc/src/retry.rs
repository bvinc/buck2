/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is dual-licensed under either the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree or the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree. You may select, at your option, one of the
 * above-listed licenses.
 */

use std::future::Future;
use std::time::Duration;

/// Configuration for gRPC retry behavior with exponential backoff.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. Set to 0 to disable retries.
    pub max_retries: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff duration between retries.
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
        }
    }
}

/// Check whether an error is retryable by looking for a `tonic::Status` with
/// a transient gRPC status code anywhere in the error chain.
pub fn is_retryable_error(err: &anyhow::Error) -> bool {
    use tonic::Code;

    for cause in err.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return matches!(
                status.code(),
                Code::Unavailable
                    | Code::DeadlineExceeded
                    | Code::ResourceExhausted
                    | Code::Internal
                    | Code::Aborted
            );
        }
    }
    false
}

/// Execute an async operation with retry logic and exponential backoff.
///
/// `op_name` is used only for logging. `f` is called on each attempt and must
/// return a new future (the closure is `Fn`, not `FnOnce`).
pub async fn retry_grpc<F, Fut, T>(
    config: &RetryConfig,
    op_name: &str,
    f: F,
) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<T>>,
{
    let mut backoff = config.initial_backoff;

    for attempt in 0..=config.max_retries {
        match f().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                let is_last = attempt == config.max_retries;
                if is_last || !is_retryable_error(&err) {
                    return Err(err);
                }
                tracing::warn!(
                    "gRPC {} failed (attempt {}/{}), retrying in {:?}: {:#}",
                    op_name,
                    attempt + 1,
                    config.max_retries + 1,
                    backoff,
                    err,
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, config.max_backoff);
            }
        }
    }

    unreachable!("loop above always returns")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering;

    use super::*;

    #[test]
    fn test_retryable_unavailable() {
        let status = tonic::Status::unavailable("server shutting down");
        let err: anyhow::Error = status.into();
        assert!(is_retryable_error(&err));
    }

    #[test]
    fn test_retryable_deadline_exceeded() {
        let status = tonic::Status::deadline_exceeded("timeout");
        let err: anyhow::Error = status.into();
        assert!(is_retryable_error(&err));
    }

    #[test]
    fn test_retryable_resource_exhausted() {
        let status = tonic::Status::resource_exhausted("rate limited");
        let err: anyhow::Error = status.into();
        assert!(is_retryable_error(&err));
    }

    #[test]
    fn test_retryable_internal() {
        let status = tonic::Status::internal("internal error");
        let err: anyhow::Error = status.into();
        assert!(is_retryable_error(&err));
    }

    #[test]
    fn test_retryable_aborted() {
        let status = tonic::Status::aborted("aborted");
        let err: anyhow::Error = status.into();
        assert!(is_retryable_error(&err));
    }

    #[test]
    fn test_not_retryable_not_found() {
        let status = tonic::Status::not_found("missing");
        let err: anyhow::Error = status.into();
        assert!(!is_retryable_error(&err));
    }

    #[test]
    fn test_not_retryable_permission_denied() {
        let status = tonic::Status::permission_denied("denied");
        let err: anyhow::Error = status.into();
        assert!(!is_retryable_error(&err));
    }

    #[test]
    fn test_not_retryable_plain_error() {
        let err = anyhow::anyhow!("something went wrong");
        assert!(!is_retryable_error(&err));
    }

    #[test]
    fn test_retryable_wrapped() {
        let status = tonic::Status::unavailable("gone");
        let err = anyhow::Error::from(status).context("wrapper context");
        assert!(is_retryable_error(&err));
    }

    #[tokio::test]
    async fn test_retry_immediate_success() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        };
        let counter = AtomicU32::new(0);
        let result = retry_grpc(&config, "test", || {
            counter.fetch_add(1, Ordering::Relaxed);
            async { Ok(42) }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_then_succeed() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        };
        let counter = AtomicU32::new(0);
        let result = retry_grpc(&config, "test", || {
            let attempt = counter.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt < 2 {
                    Err(tonic::Status::unavailable("try again").into())
                } else {
                    Ok(99)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 99);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_non_retryable_stops_immediately() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        };
        let counter = AtomicU32::new(0);
        let result: anyhow::Result<i32> = retry_grpc(&config, "test", || {
            counter.fetch_add(1, Ordering::Relaxed);
            async { Err(tonic::Status::not_found("gone forever").into()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_retry_max_retries_exhausted() {
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        };
        let counter = AtomicU32::new(0);
        let result: anyhow::Result<i32> = retry_grpc(&config, "test", || {
            counter.fetch_add(1, Ordering::Relaxed);
            async { Err(tonic::Status::unavailable("always down").into()) }
        })
        .await;
        assert!(result.is_err());
        // 1 initial + 2 retries = 3 attempts
        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_disabled_with_zero() {
        let config = RetryConfig {
            max_retries: 0,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
        };
        let counter = AtomicU32::new(0);
        let result: anyhow::Result<i32> = retry_grpc(&config, "test", || {
            counter.fetch_add(1, Ordering::Relaxed);
            async { Err(tonic::Status::unavailable("down").into()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
