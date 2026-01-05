#[cfg(test)]
mod tests {
    use regex::Regex;
    use reqwest::{Client, StatusCode};
    use tokio::time::{Duration, Instant, sleep};

    use std::{
        collections::HashMap,
        fs::File,
        io::{BufRead, BufReader, BufWriter, Write},
        path::Path,
    };

    #[derive(Debug, PartialEq, Eq, Hash)]
    enum Method {
        GET,
        PUT,
        DELETE,
        INVALID,
    }

    struct Request {
        method: Method,
        key: String,
        value: String,
    }

    struct RetryResult {
        response: reqwest::Response,
        retry_time: u128,
        retries: u128,
    }

    struct Stats {
        success_time: u128,
        retry_time: u128,
        success_count: u128,
        retry_count: u128,
    }

    const MAX_RETRIES: usize = 1000;
    const RETRY_DELAY_MS: u64 = 2000;

    async fn retry<F, Fut>(mut f: F) -> Result<RetryResult, reqwest::Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut last_resp = None;
        let mut retry_time = 0;
        let mut retries = 0;

        for attempt in 0..MAX_RETRIES {
            let start = Instant::now();
            match f().await {
                Ok(resp) => {
                    let elapsed = start.elapsed().as_micros();
                    if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
                        return Ok(RetryResult {
                            response: resp,
                            retry_time: retry_time,
                            retries: retries,
                        });
                    }

                    retry_time += elapsed;
                    retries += 1;

                    last_resp = Some(resp);
                }
                Err(e) => {
                    let elapsed = start.elapsed().as_micros();
                    retry_time += elapsed;
                    retries += 1;
                    if attempt + 1 == MAX_RETRIES {
                        return Err(e);
                    }
                }
            }

            if attempt + 1 < MAX_RETRIES {
                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }

        Ok(RetryResult {
            response: last_resp.expect("retry exhausted without response"),
            retry_time,
            retries,
        })
    }

    #[tokio::test]
    async fn test_server() {
        let test_file_path = Path::new("tests/put-delete.txt");
        let path = test_file_path.display();
        let mut stats = HashMap::<Method, Stats>::new();
        stats.insert(
            Method::GET,
            Stats {
                success_time: 0,
                retry_time: 0,
                success_count: 0,
                retry_count: 0,
            },
        );
        stats.insert(
            Method::PUT,
            Stats {
                success_time: 0,
                retry_time: 0,
                success_count: 0,
                retry_count: 0,
            },
        );
        stats.insert(
            Method::DELETE,
            Stats {
                success_time: 0,
                retry_time: 0,
                success_count: 0,
                retry_count: 0,
            },
        );
        let file = match File::open(test_file_path) {
            Err(err) => panic!("Unable to open file {}:{}", path, err),
            Ok(file) => file,
        };
        let put_get_pattern = Regex::new(r"(PUT|GET) (.+) (.+)").unwrap();
        let delete_pattern = Regex::new(r"DELETE (.+)").unwrap();
        let client = Client::new();
        let requests: Vec<Request> = BufReader::new(file)
            .lines()
            .map(|line| {
                let line_str = line.unwrap_or_default();
                match put_get_pattern.captures(&line_str) {
                    Some(captures) => {
                        let method = captures
                            .get(1)
                            .map(|m| {
                                if m.as_str() == "GET" {
                                    return Method::GET;
                                } else {
                                    return Method::PUT;
                                }
                            })
                            .unwrap();
                        let key = captures.get(2).map(|m| m.as_str()).unwrap().to_string();
                        let value = captures
                            .get(3)
                            .map(|m| {
                                return match m.as_str() != "NOT_FOUND" {
                                    true => m.as_str(),
                                    false => "",
                                };
                            })
                            .unwrap()
                            .to_string();
                        return Request { method, key, value };
                    }
                    None => match delete_pattern.captures(&line_str) {
                        Some(captures) => {
                            return Request {
                                method: Method::DELETE,
                                key: captures.get(1).map(|m| m.as_str()).unwrap().to_string(),
                                value: "".to_string(),
                            };
                        }
                        None => {
                            return Request {
                                method: Method::INVALID,
                                key: "".to_string(),
                                value: "".to_string(),
                            };
                        }
                    },
                }
            })
            .collect();

        for req in requests {
            match req.method {
                Method::GET => {
                    let start = Instant::now();
                    let result = retry(|| {
                        client
                            .get(format!("http://localhost:8000/{}", req.key))
                            .send()
                    })
                    .await
                    .unwrap();
                    let success_time = start.elapsed().as_micros() - result.retry_time;
                    let stat = stats.get_mut(&Method::GET).unwrap();
                    stat.retry_time += result.retry_time;
                    stat.retry_count += result.retries;
                    stat.success_time += success_time;
                    stat.success_count += 1;

                    let response = result.response;
                    match req.value.is_empty() {
                        true => assert_eq!(response.status(), StatusCode::NOT_FOUND),
                        false => {
                            assert_eq!(response.status(), StatusCode::OK);
                            assert_eq!(response.text().await.unwrap(), req.value);
                        }
                    }
                }
                Method::PUT => {
                    let start = Instant::now();
                    let result = retry(|| {
                        client
                            .put(format!("http://localhost:8000/{}", req.key))
                            .body(req.value.clone())
                            .send()
                    })
                    .await
                    .unwrap();
                    let success_time = start.elapsed().as_micros() - result.retry_time;
                    let stat = stats.get_mut(&Method::PUT).unwrap();
                    stat.retry_time += result.retry_time;
                    stat.retry_count += result.retries;
                    stat.success_time += success_time;
                    stat.success_count += 1;

                    let resp: reqwest::Response = result.response;
                    assert_eq!(resp.status(), StatusCode::OK);
                }
                Method::DELETE => {
                    let start = Instant::now();
                    let result = retry(|| {
                        client
                            .delete(format!("http://localhost:8000/{}", req.key))
                            .send()
                    })
                    .await
                    .unwrap();
                    let success_time = start.elapsed().as_micros() - result.retry_time;
                    let stat = stats.get_mut(&Method::DELETE).unwrap();
                    stat.retry_time += result.retry_time;
                    stat.retry_count += result.retries;
                    stat.success_time += success_time;
                    stat.success_count += 1;

                    let resp: reqwest::Response = result.response;
                    assert_eq!(resp.status(), StatusCode::OK);
                }
                Method::INVALID => println!("Skipping req"),
            };
        }

        let mut writer = BufWriter::new(File::create("stats.log").unwrap());

        for (method, stat) in &stats {
            writer
                .write_all(
                    format!(
                        "{:?}: success_count={}, retries={}, success_avg={}µs, retry_avg={}µs\n",
                        method,
                        stat.success_count,
                        stat.retry_count,
                        if stat.success_count > 0 {
                            stat.success_time / stat.success_count
                        } else {
                            0
                        },
                        if stat.retry_count > 0 {
                            stat.retry_time / stat.retry_count
                        } else {
                            0
                        },
                    )
                    .as_bytes(),
                )
                .unwrap();
        }
    }
}
