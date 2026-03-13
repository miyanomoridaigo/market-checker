#!/usr/bin/env python3
"""
カクヨム 日間ランキング ジャンル集計スクリプト
毎日自動実行でFirestoreに集計データを登録します。

【インストール】
  pip install requests beautifulsoup4 firebase-admin

【Firebase設定】
  1. Firebase Console (https://console.firebase.google.com) でプロジェクトを作成
  2. Firestore Database を有効化
  3. プロジェクト設定 > サービスアカウント > 「新しい秘密鍵を生成」でJSONをダウンロード
  4. 以下のいずれかで認証情報を設定:
     - 環境変数: export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
     - または: SERVICE_ACCOUNT_PATH 変数に直接パスを指定

【実行方法】
  python3 kakuyomu_scraper.py

【cron設定 (毎朝6時に自動実行)】
  crontab -e で以下を追加:
  0 6 * * * /usr/bin/python3 /path/to/kakuyomu_scraper.py >> /path/to/kakuyomu.log 2>&1

【Firebaseセキュリティルール (Firestore)】
  rules_version = '2';
  service cloud.firestore {
    match /databases/{database}/documents {
      match /kakuyomu_daily_rankings/{document=**} {
        allow read: if true;
        allow write: if true;  // 本番環境では認証を追加推奨
      }
    }
  }
"""

import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import sys

# =============================================
# 設定
# =============================================

# カクヨム ランキングURL
KAKUYOMU_URL = "https://kakuyomu.jp/rankings/all/daily?work_variation=all"

# FirestoreコレクションID
FIRESTORE_COLLECTION = "kakuyomu_daily_rankings"

# Firebaseプロジェクト ID (省略可)
FIREBASE_PROJECT_ID = ""  # 例: "my-kakuyomu-tracker"

# サービスアカウントJSONのパス
# 環境変数 GOOGLE_APPLICATION_CREDENTIALS が優先されます
SERVICE_ACCOUNT_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "service-account.json"
)

# ポイント計算の上限ランク (この順位以降は0pt)
MAX_RANK_FOR_POINTS = 10  # 1位=10pt, ..., 10位=1pt


# =============================================
# ポイント計算
# =============================================
def calc_points(rank: int) -> int:
    """
    1位=10pt, 2位=9pt, ..., 10位=1pt, 11位以降=0pt
    """
    return max(0, MAX_RANK_FOR_POINTS + 1 - rank)


# =============================================
# スクレイピング
# =============================================
def scrape_rankings() -> list[dict]:
    """カクヨム日間ランキングを取得してパースする"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ja,en-US;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    print(f"[INFO] ランキングページを取得中...")
    print(f"       URL: {KAKUYOMU_URL}")

    try:
        response = requests.get(KAKUYOMU_URL, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = "utf-8"
    except requests.Timeout:
        raise RuntimeError("接続タイムアウト。ネットワーク接続を確認してください。")
    except requests.HTTPError as e:
        raise RuntimeError(f"HTTPエラー {e.response.status_code}: {e}")
    except requests.ConnectionError:
        raise RuntimeError("接続エラー。ネットワーク接続を確認してください。")

    soup = BeautifulSoup(response.text, "html.parser")
    items = soup.select(".widget-work")

    if not items:
        raise RuntimeError(
            "ランキングデータが見つかりませんでした。"
            "ページ構造が変わった可能性があります。"
        )

    works = []
    rank_seen = set()

    for idx, item in enumerate(items):
        # カクヨムネクスト作品は集計対象外
        if item.select_one(".widget-kakuyomuNext-info"):
            continue

        rank_el = item.select_one(".widget-work-rank")
        title_el = item.select_one(".widget-workCard-title")
        genre_el = item.select_one(".widget-workCard-genre")

        # ランク取得
        rank_text = rank_el.get_text(strip=True) if rank_el else ""
        try:
            rank = int(rank_text)
        except (ValueError, TypeError):
            rank = idx + 1

        # 同順位の場合は通し番号で管理
        while rank in rank_seen:
            rank += 0.1  # 仮の区別用
        rank_seen.add(rank)
        rank = int(rank) if rank == int(rank) else idx + 1

        title = title_el.get_text(strip=True) if title_el else "不明"
        genre = genre_el.get_text(strip=True) if genre_el else "不明"
        points = calc_points(rank)

        works.append({
            "rank": rank,
            "title": title,
            "genre": genre,
            "points": points,
        })

    print(f"[INFO] 取得完了: {len(works)}作品")
    return works


# =============================================
# ジャンル集計
# =============================================
def aggregate_genres(works: list[dict]) -> dict:
    """ジャンルごとにポイントと作品数を集計する"""
    genres: dict[str, dict] = {}

    for w in works:
        g = w["genre"]
        if g not in genres:
            genres[g] = {"points": 0, "count": 0}
        genres[g]["points"] += w["points"]
        genres[g]["count"] += 1

    # ポイント降順でソート
    sorted_genres = dict(
        sorted(genres.items(), key=lambda x: x[1]["points"], reverse=True)
    )

    print("\n[INFO] ジャンル別集計結果:")
    print(f"{'順位':<4} {'ジャンル':<20} {'ポイント':<10} {'作品数':<8}")
    print("-" * 46)
    for i, (name, info) in enumerate(sorted_genres.items(), 1):
        marker = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f" {i}位"
        print(f"{marker:<4} {name:<20} {info['points']:<10}pt {info['count']}作品")

    return sorted_genres


# =============================================
# Firebase初期化
# =============================================
def init_firebase():
    """Firebase Admin SDKを初期化する"""
    if firebase_admin._apps:
        return  # 既に初期化済み

    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        print(f"[ERROR] サービスアカウントファイルが見つかりません: {SERVICE_ACCOUNT_PATH}")
        print("        環境変数 GOOGLE_APPLICATION_CREDENTIALS を設定するか、")
        print("        SERVICE_ACCOUNT_PATH 変数にパスを指定してください。")
        sys.exit(1)

    try:
        cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
        options = {}
        if FIREBASE_PROJECT_ID:
            options["projectId"] = FIREBASE_PROJECT_ID
        firebase_admin.initialize_app(cred, options if options else None)
        print(f"[INFO] Firebase初期化完了")
    except Exception as e:
        print(f"[ERROR] Firebase初期化失敗: {e}")
        sys.exit(1)


# =============================================
# Firestoreへの保存
# =============================================
def save_to_firestore(date_str: str, genres: dict, works: list[dict]):
    """集計データをFirestoreに保存する"""
    init_firebase()
    db = firestore.client()

    doc_ref = db.collection(FIRESTORE_COLLECTION).document(date_str)

    # 既存データの確認
    existing = doc_ref.get()
    if existing.exists:
        print(f"[WARN] {date_str} のデータが既に存在します。上書きします。")

    doc_ref.set({
        "date": date_str,
        "collectedAt": firestore.SERVER_TIMESTAMP,
        "genres": genres,
        "works": works,
    })

    print(f"\n[INFO] Firestoreに保存しました")
    print(f"       コレクション: {FIRESTORE_COLLECTION}")
    print(f"       ドキュメントID: {date_str}")


# =============================================
# メイン処理
# =============================================
def main():
    today = datetime.now().strftime("%Y-%m-%d")
    start_time = datetime.now()

    print("=" * 50)
    print(f" カクヨム ジャンル集計スクリプト")
    print(f" 実行日時: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    try:
        # 1. スクレイピング
        works = scrape_rankings()

        # 2. ジャンル集計
        genres = aggregate_genres(works)

        # 3. Firestore保存
        save_to_firestore(today, genres, works)

        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n✅ 集計完了! (所要時間: {elapsed:.1f}秒)")
        print("=" * 50)

    except RuntimeError as e:
        print(f"\n❌ エラー: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n[INFO] 中断されました")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 予期しないエラー: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
