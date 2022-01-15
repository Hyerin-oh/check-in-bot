# 사내 체크인 봇
매주 진행되는 체크인 문서 작성을 돕는 슬랙봇입니다.

매주 수요일 11시마다 실행됩니다.

```
crontab -e
0 11 * * 3 python3 run.py --config-path CONFIG_PATH
```
