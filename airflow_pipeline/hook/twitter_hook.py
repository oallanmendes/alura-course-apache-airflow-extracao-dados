from datetime import datetime, timedelta
from airflow.providers.http.hooks.http import HttpHook
import requests
import json

class TwitterHook(HttpHook):

  def __init__(self, end_time, start_time, query, conn_id=None):
    self.query = query
    self.start_time = start_time
    self.end_time = end_time
    self.conn_id = conn_id or "twitter_default"
    super().__init__(http_conn_id=self.conn_id)

  def create_url(self):
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    start_time = self.start_time
    end_time = self.end_time
    query = self.query

    tweet_fields = "author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text&expansions=author_id"
    user_fields = "author_id&user.fields=id,name,username,created_at"

    url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

    return url_raw

  def connect_to_endpoint(self, url, session):
    request = requests.Request("GET", url)
    prep = session.prepare_request(request)
    self.log.info(f"URL: {url}")

    return self.run_and_check(session, prep, {})

  def paginate(self, url_raw, session):
    json_response_list = []

    response = self.connect_to_endpoint(url_raw, session)

    json_response = response.json()
    json_response_list.append(json_response)

    while "next_token" in json_response.get("meta", {}):
      next_token = json_response['meta']['next_token']
      url = f"{url_raw}&next_token{next_token}"

      response = self.connect_to_endpoint(url, session)

      json_response = response.json()
      json_response_list.append(json_response)

    return json_response_list

  def run(self):
    session = self.get_conn()
    url_raw = self.create_url()

    return self.paginate(url_raw, session)

if __name__ == "__main__":
  TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

  end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
  start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
  query = "data science"

  for page in TwitterHook(end_time=end_time, start_time=start_time, query=query).run():
    print(json.dumps(page, indent=2, sort_keys=True))
