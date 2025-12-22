#[cfg(test)]
mod tests {
    use regex::Regex;
    use reqwest::{Client, StatusCode};
    use tokio::time::{Duration, sleep};

    use std::{
        fs::File,
        io::{BufRead, BufReader},
        path::Path,
    };

    #[derive(Debug)]
    enum Method {
        GET,
        PUT,
        INVALID,
    }

    struct Request {
        method: Method,
        key: String,
        value: String,
    }

    const MAX_RETRIES: usize = 1000;
    const RETRY_DELAY_MS: u64 = 2000;

    async fn retry<F, Fut>(mut f: F) -> Result<reqwest::Response, reqwest::Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
    {
        let mut last_resp = None;

        for attempt in 0..MAX_RETRIES {
            match f().await {
                Ok(resp) => {
                    if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
                        return Ok(resp);
                    }

                    last_resp = Some(resp);
                }
                Err(e) => {
                    if attempt + 1 == MAX_RETRIES {
                        return Err(e);
                    }
                }
            }

            if attempt + 1 < MAX_RETRIES {
                sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }

        Ok(last_resp.expect("retry exhausted without response"))
    }

    #[tokio::test]
    async fn test_server() {
        let test_file_path = Path::new("tests/put.txt");
        let path = test_file_path.display();
        let file = match File::open(test_file_path) {
            Err(err) => panic!("Unable to open file {}:{}", path, err),
            Ok(file) => file,
        };
        let pattern = Regex::new(r"(PUT|GET) (.+) (.+)").unwrap();
        let client = Client::new();
        let requests: Vec<Request> = BufReader::new(file)
            .lines()
            .map(|line| {
                let line_str = line.unwrap_or_default();
                if let Some(captures) = pattern.captures(&line_str) {
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
                } else {
                    return Request {
                        method: Method::INVALID,
                        key: "".to_string(),
                        value: "".to_string(),
                    };
                }
            })
            .collect();
        for req in requests {
            match req.method {
                Method::GET => {
                    let resp = retry(|| {
                        client
                            .get(format!("http://localhost:8000/{}", req.key))
                            .send()
                    })
                    .await;
                    match req.value.is_empty() {
                        true => {
                            assert_eq!(resp.is_ok(), true);
                            let response = resp.unwrap();
                            assert_eq!(response.status(), StatusCode::NOT_FOUND);
                        }
                        false => {
                            assert_eq!(resp.is_ok(), true);
                            let response = resp.unwrap();
                            assert_eq!(response.status(), StatusCode::OK);
                            let response_body = response.text().await.unwrap();
                            assert_eq!(response_body, req.value);
                        }
                    }
                }
                Method::PUT => {
                    let resp = retry(|| {
                        client
                            .put(format!("http://localhost:8000/{}", req.key))
                            .body(req.value.clone())
                            .send()
                    })
                    .await;
                    assert_eq!(resp.is_ok(), true);
                    assert_eq!(resp.unwrap().status(), StatusCode::OK);
                }
                Method::INVALID => println!("Skipping req"),
            };
        }
    }
}
