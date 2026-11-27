import asyncio
import logging
import os
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from unittest.mock import patch

os.environ["ADMIN_EMAIL"] = "admin@test.reloop"
os.environ["ADMIN_PASSWORD"] = "test-admin-password-with-length"

import reloop_backend as backend


class AuditFixTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="reloop-tests-")
        self.db_path = os.path.join(self.temp_dir.name, "test.db")
        backend.DB_PATH = self.db_path
        backend.init_db()
        self.user = {"id": "u-demo", "email": "minhanh@reloop.vn", "name": "Minh Anh"}
        self.admin = {"id": "admin-test", "email": "admin@test.reloop"}

    def tearDown(self):
        self.temp_dir.cleanup()

    @contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def checkout_payload(self, product_id="p-1", method="COD", voucher_id=None):
        return backend.CheckoutRequest(
            product_ids=[product_id],
            voucher_id=voucher_id,
            recipient_name="Nguyễn An",
            phone="0912345678",
            shipping_address="12 Nguyễn Huệ, Quận 1",
            payment_method=method,
        )

    def test_postgres_adapter_preserves_literal_percent_without_parameters(self):
        class FakeCursor:
            def execute(self, *args):
                self.args = args

        cursor = FakeCursor()
        backend.PostgresCursor(cursor).execute(
            "DELETE FROM wardrobe_items WHERE source_product_id LIKE 'p-seed-%'"
        )
        self.assertEqual(len(cursor.args), 1)
        self.assertIn("LIKE 'p-seed-%'", cursor.args[0])

    def test_neon_ai_gateway_uses_configured_chat_endpoint(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return b'{"choices":[{"message":{"content":"ok"}}]}'

        previous = (
            backend.NEON_AI_GATEWAY_TOKEN,
            backend.NEON_AI_GATEWAY_BASE_URL,
            backend.NEON_AI_MODEL,
        )
        backend.NEON_AI_GATEWAY_TOKEN = "test-token"
        backend.NEON_AI_GATEWAY_BASE_URL = "https://gateway.example"
        backend.NEON_AI_MODEL = "gpt-test"
        self.addCleanup(
            lambda: (
                setattr(backend, "NEON_AI_GATEWAY_TOKEN", previous[0]),
                setattr(backend, "NEON_AI_GATEWAY_BASE_URL", previous[1]),
                setattr(backend, "NEON_AI_MODEL", previous[2]),
            )
        )

        with patch.object(
            backend.urllib.request, "urlopen", return_value=FakeResponse()
        ) as urlopen:
            result = backend.call_vilao_chat_completion(
                [{"role": "user", "content": "test"}]
            )

        request = urlopen.call_args.args[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "https://gateway.example/v1/chat/completions")
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        self.assertEqual(payload["model"], "gpt-test")
        self.assertEqual(result["choices"][0]["message"]["content"], "ok")

    def test_neon_storage_upload_and_private_read_url(self):
        previous = (
            backend.AWS_ACCESS_KEY_ID,
            backend.AWS_SECRET_ACCESS_KEY,
            backend.AWS_ENDPOINT_URL_S3,
            backend.AWS_REGION,
            backend.NEON_STORAGE_CONFIGURED,
        )
        backend.AWS_ACCESS_KEY_ID = "test-access"
        backend.AWS_SECRET_ACCESS_KEY = "test-secret"
        backend.AWS_ENDPOINT_URL_S3 = "https://storage.example"
        backend.AWS_REGION = "test-region"
        backend.NEON_STORAGE_CONFIGURED = True
        self.addCleanup(
            lambda: (
                setattr(backend, "AWS_ACCESS_KEY_ID", previous[0]),
                setattr(backend, "AWS_SECRET_ACCESS_KEY", previous[1]),
                setattr(backend, "AWS_ENDPOINT_URL_S3", previous[2]),
                setattr(backend, "AWS_REGION", previous[3]),
                setattr(backend, "NEON_STORAGE_CONFIGURED", previous[4]),
            )
        )

        class FakeStorage:
            def put_object(self, **kwargs):
                self.upload = kwargs

            def generate_presigned_url(self, operation, *, Params, ExpiresIn):
                self.presigned = (operation, Params, ExpiresIn)
                return "https://storage.example/private-image"

        storage = FakeStorage()
        with patch.object(backend, "_neon_storage_client", return_value=storage):
            stored_url = backend.store_product_image(
                "data:image/png;base64,iVBORw0KGgo=",
                "product-test",
            )
            image_url = backend.resolve_product_image(stored_url)

        self.assertTrue(stored_url.startswith("neon-s3://uploads/products/product-test/"))
        self.assertEqual(storage.upload["Bucket"], "uploads")
        self.assertEqual(storage.upload["ContentType"], "image/png")
        self.assertEqual(image_url, "https://storage.example/private-image")
        self.assertEqual(storage.presigned[2], 604800)

    def test_listing_upload_uses_neon_object_key_and_signed_product_response(self):
        previous = (
            backend.AWS_ACCESS_KEY_ID,
            backend.AWS_SECRET_ACCESS_KEY,
            backend.AWS_ENDPOINT_URL_S3,
            backend.AWS_REGION,
            backend.NEON_STORAGE_CONFIGURED,
        )
        backend.AWS_ACCESS_KEY_ID = "test-access"
        backend.AWS_SECRET_ACCESS_KEY = "test-secret"
        backend.AWS_ENDPOINT_URL_S3 = "https://storage.example"
        backend.AWS_REGION = "test-region"
        backend.NEON_STORAGE_CONFIGURED = True
        self.addCleanup(
            lambda: (
                setattr(backend, "AWS_ACCESS_KEY_ID", previous[0]),
                setattr(backend, "AWS_SECRET_ACCESS_KEY", previous[1]),
                setattr(backend, "AWS_ENDPOINT_URL_S3", previous[2]),
                setattr(backend, "AWS_REGION", previous[3]),
                setattr(backend, "NEON_STORAGE_CONFIGURED", previous[4]),
            )
        )

        class FakeStorage:
            def put_object(self, **kwargs):
                self.upload = kwargs

            def generate_presigned_url(self, operation, *, Params, ExpiresIn):
                return "https://storage.example/product-image"

        storage = FakeStorage()
        payload = backend.CreateListingRequest(
            title="Áo kiểm thử",
            category="Áo",
            color="Xanh",
            material="Cotton",
            condition_score=90,
            price=100000,
            original_price=200000,
            image_url="data:image/png;base64,iVBORw0KGgo=",
        )
        with patch.object(backend, "_neon_storage_client", return_value=storage):
            result = asyncio.run(
                backend.create_marketplace_listing(payload, self.user)
            )
            products = backend.list_products(
                category=None,
                circular_only=False,
                search=None,
                sort="default",
                limit=200,
                offset=0,
            )

        self.assertIn("product_id", result)
        stored_image_url = storage.upload["Key"]
        with self.connect() as conn:
            row = conn.execute(
                "SELECT image_url FROM products WHERE id = ?",
                (result["product_id"],),
            ).fetchone()
        self.assertEqual(
            row[0],
            f"neon-s3://uploads/{stored_image_url}",
        )
        product = next(item for item in products["items"] if item["id"] == result["product_id"])
        self.assertEqual(product["image_url"], "https://storage.example/product-image")

    def test_listing_text_is_bounded_and_control_characters_are_removed(self):
        payload = backend.CreateListingRequest(
            title="Áo<img src=x onerror=alert(1)>",
            category="Áo\x00",
            size="M",
            color="Trắng",
            material="Cotton",
            condition_score=90,
            price=100000,
            original_price=200000,
            image_url="data:image/png;base64,AAAA",
        )
        self.assertEqual(payload.category, "Áo")
        self.assertEqual(payload.title, "Áo<img src=x onerror=alert(1)>")
        with self.assertRaises(Exception):
            backend.CreateListingRequest(
                title="x" * 121,
                category="Áo",
                color="Trắng",
                material="Cotton",
                condition_score=90,
                price=100000,
                original_price=200000,
                image_url="data:image/png;base64,AAAA",
            )
        with self.assertRaises(Exception):
            backend.CreateListingRequest(
                title="Áo đẹp",
                category="Áo",
                color="Trắng",
                material="Cotton",
                condition_score=90,
                price=100000,
                original_price=200000,
                image_url="data:image/svg+xml;base64,PHN2Zz4=",
            )

    def test_concurrent_checkout_creates_only_one_reservation(self):
        def place_order(_):
            try:
                return backend.checkout_order(self.checkout_payload(), self.user)
            except backend.HTTPException as error:
                return error.status_code

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(place_order, range(2)))
        self.assertEqual(sum(isinstance(result, dict) for result in results), 1)
        self.assertEqual(sum(result == 409 for result in results), 1)

        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT status FROM products WHERE id = 'p-1'").fetchone()[0],
                "reserved",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM product_reservations").fetchone()[0],
                1,
            )

    def test_voucher_redemption_rejects_invalid_values_and_debits_atomically(self):
        with self.assertRaises(Exception):
            backend.RedeemVoucherRequest(points_cost=-1, discount_amount=20000)
        with self.assertRaises(backend.HTTPException) as mismatch:
            backend.redeem_points_for_voucher(
                backend.RedeemVoucherRequest(points_cost=100, discount_amount=30000),
                self.user,
            )
        self.assertEqual(mismatch.exception.status_code, 422)

        result = backend.redeem_points_for_voucher(
            backend.RedeemVoucherRequest(points_cost=100, discount_amount=20000),
            self.user,
        )
        self.assertEqual(result["voucher"]["discount_amount"], 20000)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT green_points FROM users WHERE id = 'u-demo'").fetchone()[0],
                350,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT delta FROM points_ledger WHERE user_id = 'u-demo' ORDER BY rowid DESC LIMIT 1"
                ).fetchone()[0],
                -100,
            )

    def test_seller_cannot_buy_own_listing(self):
        with self.connect() as conn:
            conn.execute("UPDATE products SET seller_id = 'u-demo' WHERE id = 'p-1'")
        with self.assertRaises(backend.HTTPException) as error:
            backend.checkout_order(self.checkout_payload(), self.user)
        self.assertEqual(error.exception.status_code, 403)
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)

    def test_payment_reservation_expires_and_cod_completes_once(self):
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO vouchers (id, user_id, code, discount_amount, min_order, is_used, created_at)
                VALUES ('v-test', 'u-demo', 'TEST-1', 20000, 0, 0, ?)
                """,
                (datetime.utcnow().isoformat(),),
            )
        transfer = backend.checkout_order(
            self.checkout_payload(method="BANK_TRANSFER", voucher_id="v-test"),
            self.user,
        )
        self.assertEqual(transfer["order_status"], "pending_payment")
        self.assertEqual(transfer["payment_status"], "pending")
        self.assertIsNotNone(transfer["reservation_expires_at"])
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM wardrobe_items").fetchone()[0], 0)
            conn.execute(
                "UPDATE orders SET reservation_expires_at = ? WHERE id = ?",
                ("2000-01-01T00:00:00", transfer["order_id"]),
            )
        backend.get_my_orders(self.user)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT order_status FROM orders WHERE id = ?", (transfer["order_id"],)).fetchone()[0],
                "expired",
            )
            self.assertEqual(
                conn.execute("SELECT status FROM products WHERE id = 'p-1'").fetchone()[0],
                "available",
            )
            self.assertEqual(conn.execute("SELECT is_used FROM vouchers WHERE id = 'v-test'").fetchone()[0], 0)

        cod = backend.checkout_order(self.checkout_payload("p-2"), self.user)
        self.assertEqual(cod["order_status"], "processing")
        with self.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM wardrobe_items").fetchone()[0], 0)
        backend.complete_admin_cod_order(cod["order_id"], self.admin)
        backend.complete_admin_cod_order(cod["order_id"], self.admin)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT order_status FROM orders WHERE id = ?", (cod["order_id"],)).fetchone()[0],
                "completed",
            )
            self.assertEqual(
                conn.execute("SELECT status FROM products WHERE id = 'p-2'").fetchone()[0],
                "sold",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM wardrobe_items WHERE source_product_id = 'p-2'").fetchone()[0],
                1,
            )

    def test_email_phone_and_ledger_reconciliation(self):
        with self.assertRaises(backend.HTTPException) as invalid_email:
            backend.register_user(
                backend.RegisterRequest(name="Nguyễn An", email="not-an-email", password="123456")
            )
        self.assertEqual(invalid_email.exception.status_code, 422)
        self.assertEqual(
            backend.normalize_vietnamese_phone("+84 (912) 345-678"),
            "0912345678",
        )
        with self.assertRaises(backend.HTTPException):
            backend.normalize_vietnamese_phone("12345")

        with self.connect() as conn:
            balance, ledger = conn.execute(
                """
                SELECT users.green_points, COALESCE(SUM(points_ledger.delta), 0)
                FROM users LEFT JOIN points_ledger ON points_ledger.user_id = users.id
                WHERE users.id = 'u-demo' GROUP BY users.id
                """
            ).fetchone()
            self.assertEqual(balance, ledger)

    def test_expired_session_is_committed_before_unauthorized_response(self):
        stored_token = backend.hash_session_token("expired-test-token")
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                (stored_token, "u-demo", (datetime.utcnow() - timedelta(days=1)).isoformat()),
            )
        with self.assertRaises(backend.HTTPException) as error:
            backend.get_current_user("Bearer expired-test-token")
        self.assertEqual(error.exception.status_code, 401)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM sessions WHERE token = ?", (stored_token,)).fetchone()[0],
                0,
            )

    def test_seeded_demo_account_cannot_authenticate(self):
        with self.connect() as conn:
            row = conn.execute(
                "SELECT password_hash, salt FROM users WHERE id = 'u-demo'"
            ).fetchone()
        self.assertEqual(row, ("", ""))
        with self.assertRaises(backend.HTTPException) as error:
            backend.login_user(
                backend.LoginRequest(email="minhanh@reloop.vn", password="any-password")
            )
        self.assertEqual(error.exception.status_code, 401)

    def test_product_filters_are_counted_and_paged_in_the_database(self):
        filtered = backend.list_products(
            category=None,
            circular_only=False,
            search="' OR 1=1 --",
            sort="default",
            limit=2,
            offset=0,
        )
        self.assertEqual(filtered["total"], 0)
        self.assertEqual(filtered["items"], [])

        page = backend.list_products(
            category=None,
            circular_only=False,
            search=None,
            sort="default",
            limit=2,
            offset=1,
        )
        self.assertEqual(len(page["items"]), 2)
        self.assertGreaterEqual(page["total"], 3)

    def test_websocket_tickets_are_redacted_from_access_logs(self):
        for logger_name in ("uvicorn.access", "uvicorn.error"):
            record = logging.LogRecord(
                logger_name,
                logging.INFO,
                __file__,
                1,
                '%s - "WebSocket %s" [accepted]',
                ("127.0.0.1:1234", "/api/messages/ws?ticket=QA_LOG_SENTINEL"),
                None,
            )
            backend.WebSocketTicketLogFilter().filter(record)
            self.assertNotIn("QA_LOG_SENTINEL", record.getMessage())
            self.assertIn("[REDACTED]", record.getMessage())

    def test_login_attempts_are_rate_limited_per_email(self):
        payload = backend.LoginRequest(
            email="locked-account@example.vn", password="wrong-password"
        )
        for _ in range(backend.LOGIN_MAX_FAILED_ATTEMPTS - 1):
            with self.assertRaises(backend.HTTPException) as error:
                backend.login_user(payload)
            self.assertEqual(error.exception.status_code, 401)

        with self.assertRaises(backend.HTTPException) as error:
            backend.login_user(payload)
        self.assertEqual(error.exception.status_code, 429)

    def test_session_tokens_are_hashed_and_logout_revokes_them(self):
        response = backend.register_user(
            backend.RegisterRequest(
                name="QA Session User",
                email="qa-session@example.vn",
                password="test-password-2026",
            )
        )
        token = response["access_token"]
        hashed_token = backend.hash_session_token(token)

        with self.connect() as conn:
            stored_token = conn.execute(
                "SELECT token FROM sessions WHERE user_id = ?", (response["user"]["id"],)
            ).fetchone()[0]
        self.assertEqual(stored_token, hashed_token)
        self.assertNotEqual(stored_token, token)
        backend.get_current_user(f"Bearer {token}")

        backend.logout_user(f"Bearer {token}")
        with self.assertRaises(backend.HTTPException) as error:
            backend.get_current_user(f"Bearer {token}")
        self.assertEqual(error.exception.status_code, 401)

    def test_existing_plaintext_sessions_are_migrated_without_revocation(self):
        raw_token = "legacy-session-token"
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                (
                    raw_token,
                    "u-demo",
                    (datetime.utcnow() + timedelta(days=1)).isoformat(),
                ),
            )

        backend.init_db()

        hashed_token = backend.hash_session_token(raw_token)
        with self.connect() as conn:
            stored_token = conn.execute(
                "SELECT token FROM sessions WHERE user_id = 'u-demo'"
            ).fetchone()[0]
        self.assertEqual(stored_token, hashed_token)
        self.assertEqual(
            backend.get_current_user(f"Bearer {raw_token}")["id"], "u-demo"
        )

    def test_sqlite_connections_enforce_foreign_keys(self):
        with self.assertRaises(sqlite3.IntegrityError):
            with backend.get_db() as conn:
                conn.execute(
                    """
                    INSERT INTO wardrobe_items (
                        id, user_id, title, category, size, color, material,
                        original_price, paid_price, image_url, purchased_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "qa-orphan",
                        "missing-user",
                        "QA item",
                        "Áo",
                        "M",
                        "Xanh",
                        "Cotton",
                        100,
                        100,
                        "https://example.test/item.jpg",
                        datetime.utcnow().isoformat(),
                    ),
                )


if __name__ == "__main__":
    unittest.main()
