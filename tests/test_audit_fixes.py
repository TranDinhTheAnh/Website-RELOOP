import asyncio
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
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                ("expired-test-token", "u-demo", (datetime.utcnow() - timedelta(days=1)).isoformat()),
            )
        with self.assertRaises(backend.HTTPException) as error:
            backend.get_current_user("Bearer expired-test-token")
        self.assertEqual(error.exception.status_code, 401)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM sessions WHERE token = 'expired-test-token'").fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
