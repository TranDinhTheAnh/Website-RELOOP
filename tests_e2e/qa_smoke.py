"""Live API QA against an explicitly supplied local SQLite database."""

import argparse
import asyncio
import json
import os
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import websockets


PASSWORD = "qa-test-pass-2026"


class QA:
    def __init__(self, base_url, db_path, run_id, state_path):
        self.base_url = base_url.rstrip("/")
        self.db_path = Path(db_path)
        self.run_id = run_id
        self.state_path = Path(state_path)
        self.results = []

    def check(self, name, passed, evidence):
        status = "PASS" if passed else "FAIL"
        self.results.append((name, status, evidence))
        print(f"{status} | {name} | {evidence}")

    def request(self, method, path, *, token=None, body=None, timeout=30):
        headers = {}
        data = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.base_url + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                status = response.status
        except urllib.error.HTTPError as error:
            raw = error.read()
            status = error.code
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            payload = {"_raw_bytes": len(raw)}
        return status, payload

    def sql(self, query, parameters=()):
        with sqlite3.connect(self.db_path, timeout=20) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(query, parameters).fetchall()

    def register(self, role):
        email = f"qa_{self.run_id}_{role}@test.reloop.vn"
        status, response = self.request(
            "POST",
            "/api/auth/register",
            body={
                "name": f"[QA] {role} 🌿",
                "email": email,
                "password": PASSWORD,
                "height": 165,
                "weight": 55,
            },
        )
        if status != 200:
            self.check(f"register {role}", False, f"HTTP {status}: {response}")
            return None
        return {
            "role": role,
            "email": email,
            "id": response["user"]["id"],
            "token": response["access_token"],
        }

    def login(self, email):
        return self.request(
            "POST",
            "/api/auth/login",
            body={"email": email, "password": PASSWORD},
        )

    def create_product(self, seller, title, *, price=150000):
        status, response = self.request(
            "POST",
            "/api/products",
            token=seller["token"],
            body={
                "title": title,
                "category": "Áo",
                "size": "M",
                "color": "Xanh lá 🌿",
                "material": "Cotton",
                "condition_score": 90,
                "condition_label": "Rất tốt",
                "ai_note": "Sản phẩm kiểm thử QA.",
                "price": price,
                "original_price": max(price * 2, 50000),
                "image_url": "data:image/png;base64,iVBORw0KGgo=",
            },
        )
        self.check(
            f"create product {title}",
            status == 200 and bool(response.get("product_id")),
            f"HTTP {status}; product_id_present={bool(response.get('product_id'))}",
        )
        return response.get("product_id")

    def checkout(self, user, product_id, method="COD", voucher_id=None):
        return self.request(
            "POST",
            "/api/orders/checkout",
            token=user["token"],
            body={
                "product_ids": [product_id],
                "voucher_id": voucher_id,
                "recipient_name": "QA Người nhận",
                "phone": "+84 912 345 678",
                "shipping_address": "12 Đường Kiểm Thử, Quận 1",
                "payment_method": method,
            },
        )

    def _table_counts(self):
        rows = self.sql(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        names = [row["name"] for row in rows]
        counts = {}
        for table in names:
            counts[table] = self.sql(f'SELECT COUNT(*) AS n FROM "{table}"')[0]["n"]
        return names, counts

    def run(self):
        status, _ = self.request("GET", "/docs")
        self.check("Swagger docs", status == 200, f"GET /docs -> HTTP {status}")
        status, spec = self.request("GET", "/openapi.json")
        paths = spec.get("paths", {}) if isinstance(spec, dict) else {}
        self.check(
            "OpenAPI route inventory",
            status == 200 and len(paths) >= 35,
            f"GET /openapi.json -> HTTP {status}; {len(paths)} paths",
        )

        schema_tables, seed_counts = self._table_counts()
        expected_tables = {
            "users", "sessions", "products", "wardrobe_items", "orders",
            "product_reservations", "vouchers", "charity_projects",
            "points_ledger", "notifications", "complaints", "complaint_events",
            "conversations", "messages", "message_presence", "login_attempts",
        }
        self.check(
            "SQLite schema table set",
            expected_tables.issubset(schema_tables),
            f"found {len(schema_tables)} tables; required present={expected_tables.issubset(schema_tables)}",
        )
        status, products = self.request("GET", "/api/products?limit=5")
        self.check(
            "product listing pagination",
            status == 200 and len(products.get("items", [])) <= 5,
            f"HTTP {status}; total={products.get('total')}; returned={len(products.get('items', []))}",
        )
        status, injected_search = self.request(
            "GET", "/api/products?search=" + urllib.parse.quote("' OR 1=1 --")
        )
        self.check(
            "search SQL injection probe",
            status == 200 and isinstance(injected_search.get("items"), list),
            f"HTTP {status}; response contains an items list",
        )
        status, missing_product = self.request("GET", "/api/products/qa-missing-id")
        self.check(
            "unknown product",
            status == 404,
            f"GET missing product -> HTTP {status}",
        )

        status, unauth = self.request("GET", "/api/auth/me")
        self.check("auth required", status == 401, f"GET /api/auth/me without token -> HTTP {status}")
        status, bad_token = self.request("GET", "/api/auth/me", token="not-a-valid-token")
        self.check("invalid token rejected", status == 401, f"invalid bearer -> HTTP {status}")
        demo_status, _ = self.request(
            "POST",
            "/api/auth/login",
            body={"email": "minhanh@reloop.vn", "password": PASSWORD},
        )
        self.check(
            "seeded demo account disabled",
            demo_status == 401,
            f"demo login -> HTTP {demo_status}",
        )
        throttle_email = f"qa_{self.run_id}_throttle@test.reloop.vn"
        throttle_statuses = [
            self.request(
                "POST",
                "/api/auth/login",
                body={"email": throttle_email, "password": "wrong-password"},
            )[0]
            for _ in range(11)
        ]
        self.check(
            "login attempts throttled",
            throttle_statuses[:9] == [401] * 9
            and all(status == 429 for status in throttle_statuses[9:]),
            f"first statuses={throttle_statuses[:10]}; subsequent attempts={throttle_statuses[10:]}",
        )

        status, invalid_email = self.request(
            "POST",
            "/api/auth/register",
            body={
                "name": "[QA] Invalid",
                "email": f"qa_{self.run_id}_invalid",
                "password": PASSWORD,
            },
        )
        self.check("invalid email rejected", status == 422, f"register malformed email -> HTTP {status}")

        roles = ["seller", "buyer", "outsider"] + [f"racer{i}" for i in range(10)]
        users = {}
        for role in roles:
            users[role] = self.register(role)
        registered = [user for user in users.values() if user]
        self.check(
            "register 13 isolated QA users",
            len(registered) == 13,
            f"registered={len(registered)}; emails use qa_{self.run_id}_ prefix",
        )
        buyer = users["buyer"]
        seller = users["seller"]
        outsider = users["outsider"]
        racers = [users[f"racer{i}"] for i in range(10)]
        for role, user in users.items():
            if user:
                row = self.sql(
                    "SELECT green_points, password_hash, salt FROM users WHERE id=?",
                    (user["id"],),
                )[0]
                ledger = self.sql(
                    "SELECT COALESCE(SUM(delta), 0) AS total FROM points_ledger WHERE user_id=?",
                    (user["id"],),
                )[0]["total"]
                self.check(
                    f"{role} registration ledger/password",
                    row["green_points"] == 100
                    and ledger == 100
                    and row["password_hash"] != PASSWORD
                    and len(row["salt"]) >= 32,
                    f"balance={row['green_points']}; ledger={ledger}; hash_not_plaintext={row['password_hash'] != PASSWORD}; salt_length={len(row['salt'])}",
                )
        if buyer:
            status, duplicate = self.request(
                "POST",
                "/api/auth/register",
                body={
                    "name": "[QA] duplicate",
                    "email": buyer["email"].upper(),
                    "password": PASSWORD,
                },
            )
            self.check("duplicate email rejected case-insensitively", status == 400, f"HTTP {status}")
            login_status, login_response = self.login(buyer["email"])
            self.check(
                "login and session persistence",
                login_status == 200 and login_response.get("access_token"),
                f"login HTTP {login_status}; token withheld",
            )
            session = self.sql(
                "SELECT token, expires_at FROM sessions WHERE user_id=? ORDER BY expires_at DESC LIMIT 1",
                (buyer["id"],),
            )[0]
            self.check(
                "session token hashing and expiry",
                session["expires_at"] is not None
                and session["token"] != login_response.get("access_token"),
                f"token stored as hash={session['token'] != login_response.get('access_token')}; expiry_present={bool(session['expires_at'])}",
            )
            buyer["token"] = login_response["access_token"]
            _, logout_session = self.login(buyer["email"])
            logout_token = logout_session.get("access_token")
            logout_status, _ = self.request(
                "POST", "/api/auth/logout", token=logout_token
            )
            after_logout_status, _ = self.request(
                "GET", "/api/auth/me", token=logout_token
            )
            self.check(
                "logout revokes only its session",
                logout_status == 200 and after_logout_status == 401,
                f"logout HTTP {logout_status}; replay HTTP {after_logout_status}",
            )

        admin_email = os.environ.get("ADMIN_EMAIL", "")
        admin_password = os.environ.get("ADMIN_PASSWORD", "")
        admin_status, admin_response = self.request(
            "POST",
            "/api/auth/login",
            body={"email": admin_email, "password": admin_password},
        )
        admin = None
        if admin_status == 200:
            admin = {
                "id": admin_response["user"]["id"],
                "email": admin_email,
                "token": admin_response["access_token"],
            }
        self.check(
            "configured QA admin login",
            admin is not None and admin_response["user"].get("is_admin") is True,
            f"HTTP {admin_status}; is_admin={admin_response.get('user', {}).get('is_admin')}",
        )
        status, admin_denied = self.request("GET", "/api/admin/users", token=buyer["token"])
        self.check("admin endpoint denies normal user", status == 403, f"HTTP {status}")
        status, admin_unauth = self.request("GET", "/api/admin/users")
        self.check("admin endpoint denies anonymous", status == 401, f"HTTP {status}")
        if admin:
            status, admin_users = self.request("GET", "/api/admin/users", token=admin["token"])
            no_sensitive = all(
                not ({"password_hash", "salt", "token"} & set(row))
                for row in admin_users.get("items", [])
            )
            self.check(
                "admin users response excludes credentials",
                status == 200 and no_sensitive,
                f"HTTP {status}; credential fields absent={no_sensitive}",
            )

        product_id = self.create_product(seller, f"[QA] {self.run_id} Áo chính")
        product_row = self.sql(
            "SELECT seller_id, title, price, image_url, status FROM products WHERE id=?",
            (product_id,),
        )[0]
        seller_balance = self.sql(
            "SELECT green_points FROM users WHERE id=?", (seller["id"],)
        )[0]["green_points"]
        self.check(
            "product row and seller reward",
            product_row["seller_id"] == seller["id"]
            and product_row["title"].startswith("[QA]")
            and product_row["price"] == 150000
            and seller_balance == 220,
            f"seller_id_matches={product_row['seller_id'] == seller['id']}; price={product_row['price']}; seller_balance={seller_balance}",
        )
        status, detail = self.request("GET", f"/api/products/{product_id}")
        self.check(
            "product detail with Unicode fields",
            status == 200 and detail.get("color") == "Xanh lá 🌿",
            f"HTTP {status}; Unicode round-trip={detail.get('color') == 'Xanh lá 🌿'}",
        )
        status, malformed_price = self.request(
            "POST",
            "/api/products",
            token=seller["token"],
            body={
                "title": "[QA] Giá sai",
                "category": "Áo",
                "color": "Đỏ",
                "material": "Cotton",
                "condition_score": 90,
                "price": 0,
                "original_price": 100000,
                "image_url": "https://example.invalid/qa.png",
            },
        )
        self.check("invalid listing price rejected", status == 422, f"price=0 -> HTTP {status}")

        status, charities = self.request("GET", "/api/charity/projects")
        projects = charities.get("projects", [])
        self.check("charity projects API", status == 200 and bool(projects), f"HTTP {status}; projects={len(projects)}")
        if projects and outsider:
            project_id = projects[0]["id"]
            before_raised = projects[0]["raised_vnd"]
            with ThreadPoolExecutor(max_workers=2) as executor:
                donate_results = list(
                    executor.map(
                        lambda _: self.request(
                            "POST",
                            "/api/charity/donate",
                            token=outsider["token"],
                            body={"project_id": project_id, "points_to_donate": 75},
                        ),
                        range(2),
                    )
                )
            successful_donations = [item for item in donate_results if item[0] == 200]
            after_user = self.sql(
                "SELECT green_points, donated_vnd FROM users WHERE id=?",
                (outsider["id"],),
            )[0]
            after_project = self.sql(
                "SELECT raised_vnd FROM charity_projects WHERE id=?",
                (project_id,),
            )[0]["raised_vnd"]
            self.check(
                "concurrent charity donations stay within balance",
                len(successful_donations) == 1
                and after_user["green_points"] == 25
                and after_user["donated_vnd"] == 18750
                and after_project == before_raised + 18750,
                f"HTTP statuses={[item[0] for item in donate_results]}; user_points={after_user['green_points']}; donated={after_user['donated_vnd']}; raised_delta={after_project - before_raised}",
            )
        else:
            self.check("concurrent donation test", False, "No project or outsider account available")

        status, invalid_voucher = self.request(
            "POST",
            "/api/points/redeem-voucher",
            token=buyer["token"],
            body={"points_cost": -100, "discount_amount": 20000},
        )
        self.check("negative voucher cost rejected", status == 422, f"HTTP {status}")
        with ThreadPoolExecutor(max_workers=2) as executor:
            redeem_results = list(
                executor.map(
                    lambda _: self.request(
                        "POST",
                        "/api/points/redeem-voucher",
                        token=buyer["token"],
                        body={"points_cost": 100, "discount_amount": 20000, "code_prefix": "QA"},
                    ),
                    range(2),
                )
            )
        successful_redemptions = [item for item in redeem_results if item[0] == 200]
        buyer_balance = self.sql(
            "SELECT green_points FROM users WHERE id=?", (buyer["id"],)
        )[0]["green_points"]
        self.check(
            "concurrent voucher redemption",
            len(successful_redemptions) == 1 and buyer_balance >= 0,
            f"HTTP statuses={[item[0] for item in redeem_results]}; balance={buyer_balance}",
        )
        voucher = (
            successful_redemptions[0][1].get("voucher")
            if successful_redemptions
            else None
        )

        voucher_product = self.create_product(seller, f"[QA] {self.run_id} Áo voucher", price=120000)
        voucher_checkout = None
        if voucher:
            voucher_status, voucher_checkout = self.checkout(
                buyer, voucher_product, "BANK_TRANSFER", voucher["id"]
            )
            order_id = voucher_checkout.get("order_id")
            order_row = self.sql(
                "SELECT order_status, payment_status, subtotal_vnd, discount_vnd, total_vnd FROM orders WHERE id=?",
                (order_id,),
            )[0] if order_id else None
            wardrobe_count = self.sql(
                "SELECT COUNT(*) AS n FROM wardrobe_items WHERE user_id=? AND source_product_id=?",
                (buyer["id"], voucher_product),
            )[0]["n"]
            self.check(
                "voucher checkout holds item pending payment",
                voucher_status == 200
                and order_row
                and order_row["order_status"] == "pending_payment"
                and order_row["payment_status"] == "pending"
                and order_row["total_vnd"] == 100000
                and wardrobe_count == 0,
                f"HTTP {voucher_status}; order={dict(order_row) if order_row else None}; wardrobe_count={wardrobe_count}",
            )

        race_product = self.create_product(seller, f"[QA] {self.run_id} Áo race")
        with ThreadPoolExecutor(max_workers=10) as executor:
            race_results = list(
                executor.map(lambda user: self.checkout(user, race_product), racers)
            )
        winners = [(user, result) for user, result in zip(racers, race_results) if result[0] == 200]
        reservations = self.sql(
            "SELECT product_id, order_id FROM product_reservations WHERE product_id=?",
            (race_product,),
        )
        self.check(
            "10-user concurrent checkout race",
            len(winners) == 1 and len(reservations) == 1,
            f"HTTP statuses={[result[0] for result in race_results]}; successful={len(winners)}; reservation_rows={len(reservations)}",
        )
        race_order = winners[0][1][1].get("order_id") if winners else None
        race_buyer = winners[0][0] if winners else None

        repeat_product = self.create_product(seller, f"[QA] {self.run_id} Áo double-click")
        with ThreadPoolExecutor(max_workers=5) as executor:
            repeat_results = list(
                executor.map(lambda _: self.checkout(buyer, repeat_product), range(5))
            )
        repeat_success = [result for result in repeat_results if result[0] == 200]
        self.check(
            "same-user five-way double-click",
            len(repeat_success) == 1,
            f"HTTP statuses={[result[0] for result in repeat_results]}; successful={len(repeat_success)}",
        )

        if race_order and race_buyer and admin:
            complete_status, complete_result = self.request(
                "POST",
                f"/api/admin/orders/{race_order}/complete-cod",
                token=admin["token"],
                body={},
            )
            repeated_status, _ = self.request(
                "POST",
                f"/api/admin/orders/{race_order}/complete-cod",
                token=admin["token"],
                body={},
            )
            race_product_status = self.sql(
                "SELECT status FROM products WHERE id=?", (race_product,)
            )[0]["status"]
            wardrobe = self.sql(
                "SELECT COUNT(*) AS n FROM wardrobe_items WHERE user_id=? AND source_product_id=?",
                (race_buyer["id"], race_product),
            )[0]["n"]
            self.check(
                "COD completion is idempotent and fills wardrobe",
                complete_status == 200
                and repeated_status == 200
                and race_product_status == "sold"
                and wardrobe == 1,
                f"complete HTTP {complete_status}/{repeated_status}; product={race_product_status}; wardrobe_rows={wardrobe}",
            )
        else:
            self.check("COD completion", False, "No race winner or configured admin")

        convo_status, convo = self.request(
            "POST",
            "/api/conversations",
            token=buyer["token"],
            body={"product_id": product_id},
        )
        conversation_id = convo.get("id")
        self.check("create conversation", convo_status == 200 and bool(conversation_id), f"HTTP {convo_status}; id_present={bool(conversation_id)}")
        self_product_status, _ = self.request(
            "POST", "/api/conversations", token=seller["token"], body={"product_id": product_id}
        )
        self.check("seller cannot message self", self_product_status == 400, f"HTTP {self_product_status}")
        if conversation_id:
            send_status, _ = self.request(
                "POST",
                f"/api/conversations/{conversation_id}/messages",
                token=buyer["token"],
                body={"content": "QA tin nhắn có dấu tiếng Việt 🌿"},
            )
            outsider_status, _ = self.request(
                "GET",
                f"/api/conversations/{conversation_id}/messages",
                token=outsider["token"],
            )
            unread = self.sql(
                "SELECT COUNT(*) AS n FROM notifications WHERE user_id=? AND conversation_id=? AND is_read=0",
                (seller["id"], conversation_id),
            )[0]["n"]
            self.check(
                "message delivery, notification, and IDOR",
                send_status == 200 and outsider_status == 403 and unread == 1,
                f"send HTTP {send_status}; outsider read HTTP {outsider_status}; seller unread notification={unread}",
            )
            messages = self.sql(
                "SELECT content FROM messages WHERE conversation_id=?",
                (conversation_id,),
            )
            self.check(
                "message persisted with Unicode",
                len(messages) == 1 and messages[0]["content"] == "QA tin nhắn có dấu tiếng Việt 🌿",
                f"messages={len(messages)}; Unicode exact match={bool(messages) and messages[0]['content'] == 'QA tin nhắn có dấu tiếng Việt 🌿'}",
            )
            read_status, _ = self.request(
                "GET",
                f"/api/conversations/{conversation_id}/messages",
                token=seller["token"],
            )
            unread_after = self.sql(
                "SELECT COUNT(*) AS n FROM messages WHERE conversation_id=? AND recipient_id=? AND read_at IS NULL",
                (conversation_id, seller["id"]),
            )[0]["n"]
            self.check(
                "opening conversation marks messages read",
                read_status == 200 and unread_after == 0,
                f"HTTP {read_status}; unread_messages={unread_after}",
            )

        notification_status, notifications = self.request(
            "GET", "/api/notifications", token=seller["token"]
        )
        notification_items = notifications.get("items", [])
        if notification_items:
            notification_id = notification_items[0]["id"]
            owner_status, _ = self.request(
                "POST",
                f"/api/notifications/{notification_id}/read",
                token=seller["token"],
                body={},
            )
            outsider_read_status, _ = self.request(
                "POST",
                f"/api/notifications/{notification_id}/read",
                token=outsider["token"],
                body={},
            )
        else:
            owner_status, outsider_read_status = 0, 0
        self.check(
            "notification owner access control",
            notification_status == 200 and owner_status == 200 and outsider_read_status in (403, 404),
            f"list HTTP {notification_status}; owner mark HTTP {owner_status}; other user mark HTTP {outsider_read_status}",
        )

        ws_ok = False
        if conversation_id:
            buyer_ticket_status, buyer_ticket = self.request(
                "POST", "/api/messages/ws-ticket", token=buyer["token"], body={}
            )
            seller_ticket_status, seller_ticket = self.request(
                "POST", "/api/messages/ws-ticket", token=seller["token"], body={}
            )
            if buyer_ticket_status == 200 and seller_ticket_status == 200:
                ws_ok = asyncio.run(
                    self._websocket_check(
                        conversation_id,
                        buyer,
                        seller,
                        buyer_ticket["ticket"],
                        seller_ticket["ticket"],
                    )
                )
            self.check(
                "WebSocket two-way presence/typing and ticket",
                ws_ok,
                f"tickets HTTP {buyer_ticket_status}/{seller_ticket_status}; websocket_events_verified={ws_ok}",
            )
        else:
            self.check("WebSocket two-way", False, "Conversation unavailable")

        cod_product = self.create_product(seller, f"[QA] {self.run_id} Áo khiếu nại")
        complaint_order_status, complaint_order = self.checkout(buyer, cod_product, "COD")
        complaint_order_id = complaint_order.get("order_id")
        if complaint_order_id and admin:
            self.request(
                "POST",
                f"/api/admin/orders/{complaint_order_id}/complete-cod",
                token=admin["token"],
                body={},
            )
            complaint_status, complaint = self.request(
                "POST",
                "/api/complaints",
                token=buyer["token"],
                body={
                    "order_id": complaint_order_id,
                    "product_id": cod_product,
                    "reason_code": "condition_worse",
                    "description": "QA kiểm tra vòng đời khiếu nại với nội dung đủ độ dài.",
                    "desired_resolution": "refund",
                    "evidence_images": ["data:image/png;base64,iVBORw0KGgo="],
                },
            )
            complaint_id = complaint.get("complaint_id")
            outsider_complaint_status, _ = self.request(
                "GET", f"/api/complaints/{complaint_id}", token=outsider["token"]
            )
            seller_response_status, seller_response = self.request(
                "POST",
                f"/api/complaints/{complaint_id}/seller-response",
                token=seller["token"],
                body={"accepted": False, "note": "QA seller response valid note"},
            )
            escalate_status, _ = self.request(
                "POST",
                f"/api/complaints/{complaint_id}/escalate",
                token=buyer["token"],
                body={},
            )
            resolve_status, resolved = self.request(
                "POST",
                f"/api/admin/complaints/{complaint_id}/resolve",
                token=admin["token"],
                body={"resolution_type": "refund", "resolution_note": "QA admin resolved complaint"},
            )
            event_count = self.sql(
                "SELECT COUNT(*) AS n FROM complaint_events WHERE complaint_id=?",
                (complaint_id,),
            )[0]["n"]
            self.check(
                "complaint lifecycle and event timeline",
                complaint_status == 200
                and outsider_complaint_status == 403
                and seller_response_status == 200
                and escalate_status == 200
                and resolve_status == 200
                and event_count >= 4,
                f"create/seller/escalate/resolve={complaint_status}/{seller_response_status}/{escalate_status}/{resolve_status}; outsider={outsider_complaint_status}; event_rows={event_count}",
            )
        else:
            complaint_id = None
            self.check(
                "complaint lifecycle",
                False,
                f"COD checkout HTTP {complaint_order_status}; admin_available={bool(admin)}",
            )

        admin_listing_status, _ = self.request(
            "DELETE", f"/api/products/{repeat_product}", token=seller["token"]
        )
        self.check("seller cannot use admin product deletion", admin_listing_status == 403, f"HTTP {admin_listing_status}")

        ai_status, ai_response = self.request(
            "POST", "/api/ai/chat", body={"question": "QA connectivity check"}
        )
        self.check(
            "AI endpoint without configured provider",
            ai_status == 503,
            f"POST /api/ai/chat -> HTTP {ai_status}; detail={ai_response.get('detail')}",
        )
        ai_image_status, ai_image_response = self.request(
            "POST", "/api/ai/analyze-garment"
        )
        self.check(
            "AI image endpoint without configured provider",
            ai_image_status == 503,
            f"POST /api/ai/analyze-garment -> HTTP {ai_image_status}",
        )

        qa_ids = [user["id"] for user in registered]
        ledger_gaps = []
        for user_id in qa_ids:
            row = self.sql(
                "SELECT u.id, u.green_points, COALESCE(SUM(l.delta),0) AS ledger_total FROM users u LEFT JOIN points_ledger l ON l.user_id=u.id WHERE u.id=? GROUP BY u.id",
                (user_id,),
            )[0]
            if row["green_points"] != row["ledger_total"]:
                ledger_gaps.append(
                    (row["id"], row["green_points"], row["ledger_total"])
                )
        self.check(
            "QA users points/ledger reconciliation",
            not ledger_gaps,
            f"users_checked={len(qa_ids)}; mismatches={len(ledger_gaps)}",
        )
        final_tables, final_counts = self._table_counts()
        print("SQL SNAPSHOT | table row counts: " + json.dumps(final_counts, sort_keys=True))

        state = {
            "run_id": self.run_id,
            "users": [{"role": user["role"], "email": user["email"]} for user in registered],
            "product_id": product_id,
            "race_product_id": race_product,
            "race_order_id": race_order,
            "race_buyer_email": race_buyer["email"] if race_buyer else None,
            "conversation_id": conversation_id,
            "complaint_id": complaint_id,
            "voucher_order_id": voucher_checkout.get("order_id") if voucher_checkout else None,
        }
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
        failures = [item for item in self.results if item[1] == "FAIL"]
        print(f"SUMMARY | passed={len(self.results) - len(failures)}; failed={len(failures)}; state_file={self.state_path}")
        return 1 if failures else 0

    async def _websocket_check(
        self, conversation_id, buyer, seller, buyer_ticket, seller_ticket
    ):
        base = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        async with websockets.connect(
            f"{base}/api/messages/ws",
            subprotocols=["reloop-auth", buyer_ticket],
        ) as buyer_ws:
            async with websockets.connect(
                f"{base}/api/messages/ws",
                subprotocols=["reloop-auth", seller_ticket],
            ) as seller_ws:
                await buyer_ws.send(
                    json.dumps(
                        {
                            "type": "typing",
                            "conversation_id": conversation_id,
                            "is_typing": True,
                        }
                    )
                )
                typing = json.loads(await asyncio.wait_for(seller_ws.recv(), timeout=5))
                await buyer_ws.send(json.dumps({"type": "ping"}))
                pong = json.loads(await asyncio.wait_for(buyer_ws.recv(), timeout=5))
                status, _ = await asyncio.to_thread(
                    self.request,
                    "POST",
                    f"/api/conversations/{conversation_id}/messages",
                    token=buyer["token"],
                    body={"content": "QA realtime WebSocket event"},
                )
                message = json.loads(await asyncio.wait_for(seller_ws.recv(), timeout=5))
                return (
                    typing.get("type") == "typing"
                    and pong.get("type") == "pong"
                    and status == 200
                    and message.get("type") == "message"
                )

    def verify_after_restart(self):
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        user = next(item for item in state["users"] if item["role"] == "buyer")
        status, login = self.login(user["email"])
        if status != 200:
            self.check("restart login", False, f"HTTP {status}")
            return 1
        token = login["access_token"]
        me_status, me = self.request("GET", "/api/auth/me", token=token)
        detail_status, detail = self.request(
            "GET", f"/api/products/{state['product_id']}"
        )
        orders_status, orders = self.request(
            "GET", "/api/orders/me", token=token
        )
        conversations_status, conversations = self.request(
            "GET", "/api/conversations", token=token
        )
        self.check(
            "data readable after server restart",
            me_status == 200
            and detail_status == 200
            and orders_status == 200
            and conversations_status == 200
            and any(c["id"] == state["conversation_id"] for c in conversations.get("items", [])),
            f"auth/products/orders/conversations HTTP={me_status}/{detail_status}/{orders_status}/{conversations_status}",
        )
        checks = {
            "users": self.sql("SELECT COUNT(*) AS n FROM users WHERE email LIKE ?", (f"qa_{self.run_id}_%@test.reloop.vn",))[0]["n"],
            "products": self.sql(
                "SELECT COUNT(*) AS n FROM products WHERE title LIKE ?",
                (f"%{self.run_id}%",),
            )[0]["n"],
            "orders": self.sql("SELECT COUNT(*) AS n FROM orders WHERE id IN (?, ?, ?)", (state["race_order_id"] or "", state["voucher_order_id"] or "", ""))[0]["n"],
            "messages": self.sql("SELECT COUNT(*) AS n FROM messages WHERE conversation_id=?", (state["conversation_id"],))[0]["n"] if state["conversation_id"] else 0,
            "complaint_events": self.sql("SELECT COUNT(*) AS n FROM complaint_events WHERE complaint_id=?", (state["complaint_id"],))[0]["n"] if state["complaint_id"] else 0,
        }
        self.check(
            "direct SQL persistence across restart",
            checks["users"] >= 13 and checks["products"] >= 3 and checks["messages"] >= 1,
            json.dumps(checks, sort_keys=True),
        )
        return sum(1 for _, result, _ in self.results if result == "FAIL") > 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--verify-restart", action="store_true")
    args = parser.parse_args()
    qa = QA(args.base_url, args.db_path, args.run_id, args.state_file)
    return qa.verify_after_restart() if args.verify_restart else qa.run()


if __name__ == "__main__":
    raise SystemExit(main())
