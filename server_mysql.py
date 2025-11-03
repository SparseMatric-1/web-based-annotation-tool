#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flask backend using MySQL (PyMySQL)

Endpoints
- POST /api/login            验证登录（login 表）
- GET  /api/items            读取条目（full_table1 表，按用户回填已保存修订/确认）
- GET  /api/audio            按本地路径流式返回音频
- POST /api/annotations      存储修订（output 表，id_$oid、username、fix_text、confirmed、timestamp）

依赖：  pip install flask pymysql
启动：  python server_mysql.py
访问：  file:///C:/Users/Administrator.DESKTOP-FKALNNG/Desktop/web.html
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, List

from flask import Flask, jsonify, request, send_file
import pymysql
from pymysql.cursors import DictCursor
import mimetypes
import os.path as osp


# ---- 配置 ----
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Bethel_Chengdu")
DB_NAME = os.getenv("DB_NAME", "tool")


def get_conn():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=DictCursor,
    )


app = Flask(__name__)


# ---- 简易 CORS 允许（便于直接从本地 HTML 访问） ----
@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = request.headers.get("Origin", "*") or "*"
    resp.headers["Vary"] = "Origin"
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


@app.route("/api/<path:sub>", methods=["OPTIONS"])
def opt_any(sub):
    return ("", 204)


def ensure_output_table():
    sql_create = (
        """
        CREATE TABLE IF NOT EXISTS `output` (
          `id_$oid` VARCHAR(32) NOT NULL,
          `username` VARCHAR(64) NOT NULL DEFAULT '',
          `fix_text` TEXT NOT NULL,
          `confirmed` TINYINT(1) NOT NULL DEFAULT 0,
          `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                      ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id_$oid`, `username`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
    )
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 创建表（若不存在）
            cur.execute(sql_create)


@app.before_request
def _init():
    try:
        ensure_output_table()
    except Exception:
        # 初始失败不阻塞请求，运行时会重试
        pass


# ---- 登录 ----
@app.post("/api/login")
def api_login():
    data = request.get_json(silent=True) or request.form or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "missing credentials"}), 400

    sql = "SELECT 1 AS ok FROM `login` WHERE `username`=%s AND `password`=%s LIMIT 1"
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username, password))
                row = cur.fetchone()
                if row:
                    return jsonify({"ok": True})
                else:
                    return jsonify({"error": "invalid username or password"}), 401
    except Exception as e:
        app.logger.exception("/api/login error")
        return jsonify({"error": f"server error: {e}"}), 500


# ---- 读取条目 ----
@app.get("/api/items")
def api_items():
    # 分页参数（offset/limit），默认每页 30 条
    try:
        limit = int(request.args.get("limit", 30))
        offset = int(request.args.get("offset", 0))
    except Exception:
        limit, offset = 30, 0
    limit = max(1, min(limit, 1000))
    offset = max(0, offset)

    # 用户名用于恢复个人进度（未提供则不回填）
    username = (request.args.get("username", default="") or "").strip()
    hide_confirmed = (request.args.get("hideConfirmed", default="0") or "0").lower() in ("1","true","yes")
    # 仅返回在 `output` 表中记录数 < 3 的 id_$oid（跨用户计数），并按从小到大排序
    # 同时保留与当前用户的个人保存记录（o_user）用于回填
    sql = (
        "SELECT f.`id_$oid`, f.`stage_audio_path`, f.`score_audio_input_text`, "
        "       o_user.`fix_text` AS `saved_fix_text`, o_user.`confirmed` AS `saved_confirmed` "
        "FROM `full_table1` f "
        "LEFT JOIN `output` o_user ON o_user.`id_$oid` = f.`id_$oid` AND o_user.`username` = %s "
        "LEFT JOIN (SELECT `id_$oid`, COUNT(*) AS cnt FROM `output` GROUP BY `id_$oid`) o_cnt "
        "       ON o_cnt.`id_$oid` = f.`id_$oid` "
        "WHERE f.`score_audio_input_text` IS NOT NULL "
        "  AND TRIM(f.`score_audio_input_text`) <> '' "
        "  AND UPPER(f.`score_audio_input_text`) <> 'NONE' "
        "  AND COALESCE(o_cnt.cnt, 0) < 3 "
        + ("  AND (o_user.`confirmed` IS NULL OR o_user.`confirmed` <> 1) " if hide_confirmed else "") +
        "ORDER BY f.`id_$oid` ASC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username,))
                rows: List[Dict[str, Any]] = cur.fetchall() or []
    except Exception:
        app.logger.exception("/api/items query error")
        return jsonify([])

    items: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        oid = r.get("id_$oid")
        oid = str(oid) if oid is not None else str(i + 1)

        stage_path = r.get("stage_audio_path") or ""
        if stage_path:
            audio_url = f"https://content-pic3.oss-cn-hangzhou.aliyuncs.com" + stage_path
        else:
            audio_url = ""

        name = osp.basename(stage_path) if stage_path else f"音频#{i+1}"

        # 展示文本采用 score_audio_input_text，若有保存则覆盖
        input_text = r.get("score_audio_input_text") or ""
        saved_fix = r.get("saved_fix_text")
        saved_conf = r.get("saved_confirmed")
        fix_text = saved_fix if (saved_fix is not None and str(saved_fix) != "") else input_text
        confirmed = bool(saved_conf) if (saved_conf is not None) else False

        items.append(
            {
                "id": oid,
                "id_$oid": oid,
                "name": name,
                "audio": audio_url,
                "stageAudioPath": stage_path,
                "inputText": input_text,
                "fixText": fix_text,
                "confirmed": confirmed,
            }
        )

    return jsonify(items)


@app.get("/api/audio")
def api_audio():
    """按给定本地路径返回音频内容。前端在 <audio> src 中直接使用该接口。"""
    path = request.args.get("path", type=str, default="")
    if not path:
        return jsonify({"error": "missing path"}), 400
    if not osp.exists(path) or not osp.isfile(path):
        return jsonify({"error": "file not found"}), 404
    mime, _ = mimetypes.guess_type(path)
    try:
        return send_file(path, mimetype=mime or "application/octet-stream", as_attachment=False, conditional=True)
    except Exception as e:
        app.logger.exception("/api/audio error")
        return jsonify({"error": f"open error: {e}"}), 500


# ---- 存储修订 ----
@app.post("/api/annotations")
def api_annotations():
    try:
        data = request.get_json(silent=True) or {}
        items = data.get("items") or []
        if not isinstance(items, list):
            return jsonify({"error": "invalid payload"}), 400

        ensure_output_table()
        sql = (
            "INSERT INTO `output` (`id_$oid`, `username`, `fix_text`, `confirmed`, `timestamp`) "
            "VALUES (%s, %s, %s, %s, NOW()) "
            "ON DUPLICATE KEY UPDATE `fix_text`=VALUES(`fix_text`), `confirmed`=VALUES(`confirmed`), `timestamp`=NOW()"
        )

        saved = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                top_username = (data.get("username") or "").strip()
                for it in items:
                    oid = str(it.get("id") or it.get("id_$oid") or "").strip()
                    if not oid:
                        continue
                    fix_text = it.get("fixText") or it.get("fix_text") or ""
                    username = (it.get("username") or top_username or "").strip()
                    confirmed = 1 if bool(it.get("confirmed")) else 0
                    cur.execute(sql, (oid, username, fix_text, confirmed))
                    saved += 1
        return jsonify({"saved": saved})
    except Exception as e:
        app.logger.exception("/api/annotations error")
        return jsonify({"error": f"server error: {e}"}), 500


@app.get("/api/health")
def api_health():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return jsonify({"ok": True, "db": "connected"})
    except Exception as e:
        return jsonify({"ok": False, "db": f"error: {e}"}), 500


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
    )
    port = int(os.getenv("PORT", "5000"))
    app.logger.info(
        "Starting server on 0.0.0.0:%s (DB=%s@%s:%s/%s)",
        port,
        DB_USER,
        DB_HOST,
        DB_PORT,
        DB_NAME,
    )
    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBye")
