import argparse
import json
import logging
import random
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from slack_sdk import WebClient

parser = argparse.ArgumentParser()
parser.add_argument("--config-path", type=str, required=True, help="실행에 필요한 정보들이 담긴 config 파일 경로")


def make_person_dict(notion_api_token: str, notion_version: str):
    """
    workspace 내 user들의 이름과 id를 mapping 하는 dictionary를 만들어 반환합니다.
    """
    user_dict = {}
    response = requests.get(
        "https://api.notion.com/v1/users",
        headers={
            "Authorization": f"Bearer {notion_api_token}",
            "Notion-Version": notion_version,
        },
    )
    response.raise_for_status()

    for person_info in response.json()["results"]:
        user_dict[person_info["name"]] = person_info["id"]
    return user_dict


def retrieve_databases(cfg) -> json:
    """
    원하는 조건(체크인 문서)에 맞춰 필터링 된 Database 중 가장 최근에 생성된 page 1개에 대한 정보를 가져옵니다.
    """
    url = f"https://api.notion.com/v1/databases/{cfg['database_id']}/query"
    filter = {
        "and": [
            {"property": "Tags", "multi_select": {"contains": cfg["team_name"]}},
            {"property": "Tags", "multi_select": {"contains": "OKR"}},
            {"property": "회의 유형", "multi_select": {"contains": "Check-in"}},
        ]
    }
    sort = [{"timestamp": "created_time", "direction": "descending"}]
    response = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {cfg['notion_api_token']}",
            "Notion-Version": cfg["notion_version"],
        },
        json={"filter": filter, "sorts": sort, "page_size": 1},
    )
    response.raise_for_status()
    return response.json()


def check_already_made(cfg):
    """
    retrieve_databases 함수를 통해 가장 최근에 만들어진 문서에 대한 정보를 가져옵니다.
    이후 만들어진 지 1주일이 되지 않았다면 False를 , 1주일이 되었다면 True를 반환합니다.
    """
    data = retrieve_databases(cfg)["results"][0]

    latest_created_time = datetime.strptime(data["created_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
    created_time_diff = (datetime.today() - latest_created_time).days
    if created_time_diff < 0:
        # 최근 체크인 문서가 작성된 지 일주일도 되지 않았다는 것은 사람이 직접 만들었다는 뜻으로 작성하지 않음.
        return True, None, None

    latest_title = data["properties"]["이름"]["title"][0]["plain_text"]
    latest_quater = data["properties"]["Quarter"]["multi_select"][0]["name"]
    latest_checkin_day = re.search("[0-9]{6}", latest_title).group()
    latest_index = int(re.search("#([0-9]{1,})", latest_title).group(1))
    return False, latest_checkin_day, latest_index, latest_quater


def create_pages(
    cfg: Dict[str, Any],
    latest_checkin_day: str,
    latest_index: int,
    latest_quater: str,
) -> str:
    """
    새로운 체크인 문서를 생성합니다.
    체크인 문서의 Quarter는 이전 체크인 문서의 Quarter를 , 제목의 #n 은 이전 체크인 문서의 인덱스+1 이 됩니다.
    새로 생성한 체크인 문서의 url을 리턴해줍니다.
    """
    # 주최자는 회의록 작성자가 될 수 없습니다.
    user_name2id = make_person_dict(cfg["notion_api_token"], cfg["notion_version"])
    writer_list = [user_name2id[name] for name in cfg["people_list"]]
    writer_list.remove(user_name2id[cfg["host"]])

    check_in_day = datetime.strptime(latest_checkin_day, "%y%m%d") + timedelta(weeks=1)
    title = f"[{check_in_day.strftime('%y%m%d')}] {cfg['base_title']}{latest_index + 1}"
    payload = {
        "parent": {"database_id": cfg["database_id"]},
        "properties": {
            "이름": {"title": [{"type": "text", "text": {"content": title}}]},
            "날짜": {"date": {"start": check_in_day.strftime(("%Y-%m-%d"))}},
            "Quarter": {"multi_select": [{"name": latest_quater}]},
            "주최자": {"people": [{"object": "user", "id": user_name2id[cfg["host"]]}]},
            "참석자": {"people": [{"object": "user", "id": user_name2id[id]} for id in cfg["people_list"]]},
            "회의록 작성자": {"people": [{"object": "user", "id": random.choice(writer_list)}]},
            "Tags": {"multi_select": [{"name": "OKR"}, {"name": cfg["team_name"]}]},
            "회의 유형": {"multi_select": [{"name": "Check-in"}]},
        },
    }
    response = requests.post(
        "https://api.notion.com/v1/pages",
        headers={
            f"Authorization": f"Bearer {cfg['notion_api_token']}",
            "Notion-Version": cfg["notion_version"],
        },
        json=payload,
    )
    response.raise_for_status()

    return response.json()["url"]


def main(args: argparse.Namespace):
    """
    해당 script는 cron job에 의해 매주 특정 요일마다 실행됩니다.
    filter를 이용해 database 중 가장 최신 문서를 불러옵니다.
    이후 가장 최신 문서가 실행 당일 전 일주일 내에 만들어졌다면 사람이 직접 만든 것으로 판단해 문서를 만들지 않습니다.
    일주일 이상 차이나는 경우에는 새로운 문서를 만듭니다.
    이후 문서 url과 함께 정해진 슬랙채널에 메시지를 보냅니다.
    """
    with open(args.config_path) as f:
        cfg = json.load(f)
    necessary_key = [
        "slack_bot_token",
        "notion_api_token",
        "database_id",
        "notion_version",
        "channel_id",
        "team_name",
        "base_title",
        "host",
        "people_list",
    ]

    for key in necessary_key:
        assert key in cfg.keys(), f"config에는 {key}가 필요합니다. 추가해서 다시 시도해주세요."

    client = WebClient(token=cfg["slack_bot_token"])

    logging.info("[+] 일주일 내 이미 만들어진 문서가 있는 지 확인합니다.")
    made, latest_checkin_day, latest_index, latest_quater = check_already_made(cfg)
    if made:
        logging.info("[+] 일주일 내 이미 만들어진 문서가 존재하므로 체크인 문서를 새로 생성하지 않습니다.")
        response = client.chat_postMessage(
            channel=cfg["channel_id"], text=f"이번 주 {cfg['team_name']} 체크인 문서는 별도로 생성되지 않았습니다."
        )
        response.validate()

    else:
        logging.info("[+] 일주일 내 이미 만들어진 문서가 존재하지 않으므로 체크인 문서를 새로 생성합니다.")
        new_page_url = create_pages(cfg, latest_checkin_day, latest_index, latest_quater)
        response = client.chat_postMessage(
            channel=cfg["channel_id"],
            text=f"이번 주 {cfg['team_name']} 체크인 문서입니다. template을 클릭해 작성해주세요! \n {new_page_url}",
        )
        response.validate()


if __name__ == "__main__":
    main(parser.parse_args())