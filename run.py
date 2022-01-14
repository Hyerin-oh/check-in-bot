import os
import random
from datetime import datetime
from typing import Any, Dict, List

import requests
from slack_sdk import WebClient

from check_in_bot.notion import check_already_made, create_pages

## notion
notion_token = "notion_token"
parents_database_id = "parents_database_id"

## slack
slack_token = "slack_token"
channel = "channel"

client = WebClient(token=slack_token)

def retrieve_databases(database_id: int, notion_api_key: str, filetering: Dict, notion_version: str = "2021-08-16"):
    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {notion_api_key}",
            "Notion-Version": notion_version,
            "Content-Type": "application/json",
        },
        json={"filter": filtering, "sorts": [{"timestamp": "created_time", "direction": "descending"}], "page_size": 1},
    ).json()
    return response

def check_already_made(database_id: int, notion_api_key: str, filetering: Dict, notion_version: str = "2021-08-16"):
    """
    필터링을 통해 database에서 가장 최신에 만들어진 체크인 문서를 읽어들입니다.
    이후 만들어진 지 1주일이 되지 않았다면 False를 , 1주일이 되었다면 True를 반환합니다.
    """
    data = retrieve_databases(database_id, notion_api_key , filtering, notion_version)["results"][0]

    # created_time의 형식이 2022-01-14T11:59:00.000Z 이라서 T 앞부분만 사용
    latest_created_time = data["created_time"].split("T")[0]
    latest_created_time = datetime.strptime(latest_created_time , "%Y-%m-%d")
    created_time_diff = (datetime.today() - latest_created_time).days

    if created_time_diff < 7:
        # 최근 체크인 문서가 작성된 지 일주일도 되지 않았다는 것은 사람이 직접 만들었다는 뜻으로 작성하지 않음.
        return True, _, _

    latest_index = int(data["url"].split('-')[6])
    latest_quater = result["properties"]["Quarter"]['multi_select'][0]['name']
    return False , latest_index, latest_quater

def choice_writer(host:str, people_list:List[str]):
    """
    참석자 명단에서 랜덤으로 한 명을 뽑아 회의록 작성자를 정합니다.
    주최자는 회의록 작성자가 될 수 없습니다.
    """
    writer = host
    while writer == host:
        writer = random.choice(people_list)
    return writer

def create_pages(latest_index: int, latest_quater: str, host:str, people_list: List[str], parents_database_id: str, base_title: str):
    people_payload = {"people" : [{"object" : "user" , "id" : id} for id in people_list]}
    title = f"[{datetime.today().strftime('%y%d%m')}] {base_title} #{latest_index}"
    payload = {
        "parent": { "database_id": parents_database_id },
        "properties": {
            "이름" : {"title" : [{"type":"text" , "text" : {"content" : title}}]}
            "날짜" : {"date" : {"start" : datetime.today().strftime('%Y-%m-%dT%H:%M:%S.%f%z')}},
            "Quarter" : {"multi_select" : [{"name" : latest_quater}]},
            "주최자" : {"people" : [{"object" : "user" , "id" : host}]},
            "참석자" : people_payload,
            "회의록 작성자" : {"people" : [{"object" : "user" , "id" : choice_writer(host, people_list)}]},
            "Tags" : {"multi_select" : [{"name" : "OKR"} , {"name"} : "MLE"]},
            "회의 유형" : {"multi_select" : [{"name" : "Check-in"}]},
        }
    }
   response = requests.post(
        'https://api.notion.com/v1/pages',
        headers = {
            f"Authorization": f"Bearer {notion_api_token}",
            "Content-Type": "application/json",
            "Notion-Version": notino_version,
        },
        json=payload
    )
    response.raise_status()
    return True

def main():
    print("일주일 내 이미 만들어진 문서가 있는 지 확인합니다.")
    made, latest_index, latest_quater = check_already_made()
    if made:
        print("더 이상 만들지 않습니다.")
        client.postMessage(channe_id=channel, text="더 이상 만들지 않습니다.")
    else:
        url = create_pages()
        client.postMessage(channe_id=channel, text=f"다 만들어졌어요. {url} 이제 쓰러 가세요")


if __name__ == "__main__":
    main()
