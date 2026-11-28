import asyncio
import io
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
        self.order_email_patch = patch.object(
            backend,
            "send_order_notification_emails",
            return_value=None,
        )
        self.order_email_patch.start()
        self.addCleanup(self.order_email_patch.stop)
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

    def insert_user(self, user_id, email):
        created_at = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (id, name, email, password_hash, salt, created_at)
                VALUES (?, 'Tài khoản kiểm thử', ?, '', '', ?)
                """,
                (user_id, email, created_at),
            )
        return {"id": user_id, "email": email, "name": "Tài khoản kiểm thử"}

    def test_admin_can_hard_delete_user_and_owned_data(self):
        target = self.insert_user("u-delete-test", "delete-test@example.vn")
        with self.connect() as conn:
            conn.execute("UPDATE products SET seller_id = ? WHERE id = 'p-1'", (target["id"],))
            conn.execute(
                "UPDATE products SET seller_id = ?, image_url = ? WHERE id = 'p-4'",
                (target["id"], "neon-s3://uploads/products/p-4/test.jpg"),
            )
            conn.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                ("test-delete-session", target["id"], "2099-01-01T00:00:00"),
            )
            conn.execute(
                """
                INSERT INTO password_reset_tokens (user_id, token_hash, expires_at, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    target["id"],
                    "delete-test-reset-hash",
                    "2099-01-01T00:00:00",
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.execute(
                "INSERT INTO login_attempts VALUES (?, 1, ?)",
                (
                    backend.hashlib.sha256(target["email"].encode("utf-8")).hexdigest(),
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.execute(
                """
                INSERT INTO wardrobe_items (
                    id, user_id, source_product_id, title, category, size, color, material,
                    cycle, original_price, paid_price, image_url, reconsigned, purchased_at
                ) VALUES ('wardrobe-delete', ?, 'p-1', 'Áo thử', 'Áo', 'M', 'Đen', 'Cotton',
                          1, 100000, 50000, 'https://example.test/item.jpg', 0, ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO wardrobe_items (
                    id, user_id, source_product_id, title, category, size, color, material,
                    cycle, original_price, paid_price, image_url, reconsigned, purchased_at
                ) VALUES ('wardrobe-other', 'u-demo', 'p-1', 'Áo đã mua', 'Áo', 'M', 'Đen',
                          'Cotton', 1, 100000, 50000, 'https://example.test/item.jpg', 0, ?)
                """,
                (datetime.utcnow().isoformat(),),
            )
            conn.execute(
                """
                INSERT INTO vouchers (id, user_id, code, discount_amount, min_order, is_used, created_at)
                VALUES ('voucher-delete', ?, 'DELETE-1', 10000, 0, 0, ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO points_ledger (id, user_id, title, delta, entry_type, created_at)
                VALUES ('ledger-delete', ?, 'Điểm thử', 10, 'earn', ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO message_presence (user_id, last_seen_at) VALUES (?, ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO notifications (
                    id, user_id, notification_type, title, message, buyer_email, created_at
                ) VALUES ('notification-delete', 'u-demo', 'order_received', 'Đơn thử',
                          'Thông báo thử', ?, ?)
                """,
                (target["email"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO conversations (id, product_id, buyer_id, seller_id, created_at, updated_at)
                VALUES ('conversation-delete', 'p-3', ?, 'u-demo', ?, ?)
                """,
                (target["id"], datetime.utcnow().isoformat(), datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO messages (id, conversation_id, sender_id, recipient_id, content, created_at)
                VALUES ('message-delete', 'conversation-delete', ?, 'u-demo', 'Tin nhắn thử', ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )

        order = backend.checkout_order(self.checkout_payload("p-2"), target)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO payments (id, order_id, method, amount_vnd, status, created_at)
                VALUES ('payment-delete', ?, 'COD', 50000, 'unpaid', ?)
                """,
                (order["order_id"], datetime.utcnow().isoformat()),
            )
            conn.execute(
                """
                INSERT INTO complaints (
                    id, order_id, product_id, buyer_id, seller_id, reason_code, description,
                    desired_resolution, evidence_json, product_snapshot_json, status,
                    created_at, updated_at
                ) VALUES ('complaint-delete', ?, 'p-2', ?, 'u-demo', 'other', 'Mô tả thử',
                          'refund', '[]', '{}', 'open', ?, ?)
                """,
                (
                    order["order_id"],
                    target["id"],
                    datetime.utcnow().isoformat(),
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.execute(
                """
                INSERT INTO complaint_events (
                    id, complaint_id, actor_id, actor_role, event_type, note, created_at
                ) VALUES ('complaint-event-delete', 'complaint-delete', ?, 'buyer',
                          'created', 'Tạo khiếu nại thử', ?)
                """,
                (target["id"], datetime.utcnow().isoformat()),
            )

        with patch.object(backend, "delete_stored_product_image", return_value=True) as delete_image:
            result = backend.delete_registered_user(target["id"], self.admin)

        self.assertEqual(result["user_id"], target["id"])
        self.assertEqual(result["archived_products"], 1)
        self.assertIsNone(result["storage_cleanup_warning"])
        delete_image.assert_called_once_with("neon-s3://uploads/products/p-4/test.jpg")
        with self.connect() as conn:
            self.assertIsNone(conn.execute("SELECT id FROM users WHERE id = ?", (target["id"],)).fetchone())
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM payments WHERE id = 'payment-delete'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM order_status_history WHERE order_id = ?", (order["order_id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM product_reservations WHERE order_id = ?", (order["order_id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT status FROM products WHERE id = 'p-2'").fetchone()[0], "available")
            archived_product = conn.execute(
                "SELECT seller_id, status FROM products WHERE id = 'p-1'"
            ).fetchone()
            self.assertEqual(archived_product, (None, "removed"))
            self.assertIsNone(conn.execute("SELECT id FROM products WHERE id = 'p-4'").fetchone())
            self.assertEqual(
                conn.execute(
                    "SELECT source_product_id FROM wardrobe_items WHERE id = 'wardrobe-other'"
                ).fetchone()[0],
                "p-1",
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM wardrobe_items WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM conversations WHERE id = 'conversation-delete'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM messages WHERE id = 'message-delete'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM complaints WHERE id = 'complaint-delete'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM complaint_events WHERE id = 'complaint-event-delete'").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM notifications WHERE buyer_email = ?", (target["email"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM sessions WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM login_attempts WHERE identifier_hash = ?", (backend.hashlib.sha256(target["email"].encode("utf-8")).hexdigest(),)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM vouchers WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM points_ledger WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM message_presence WHERE user_id = ?", (target["id"],)).fetchone()[0], 0)

    def test_admin_delete_rejects_non_admin_admin_target_and_active_sale(self):
        target = self.insert_user("u-delete-blocked", "delete-blocked@example.vn")
        with self.assertRaises(backend.HTTPException) as unauthorized:
            backend.delete_registered_user(target["id"], self.user)
        self.assertEqual(unauthorized.exception.status_code, 403)

        admin_id = None
        with self.connect() as conn:
            admin_id = conn.execute(
                "SELECT id FROM users WHERE LOWER(email) = ?",
                (backend.ADMIN_EMAIL,),
            ).fetchone()[0]
        with self.assertRaises(backend.HTTPException) as protected_admin:
            backend.delete_registered_user(admin_id, self.admin)
        self.assertEqual(protected_admin.exception.status_code, 409)

        with self.connect() as conn:
            conn.execute("UPDATE products SET seller_id = ? WHERE id = 'p-1'", (target["id"],))
        order = backend.checkout_order(self.checkout_payload("p-1"), self.user)
        with self.assertRaises(backend.HTTPException) as active_order:
            backend.delete_registered_user(target["id"], self.admin)
        self.assertEqual(active_order.exception.status_code, 409)
        with self.connect() as conn:
            self.assertIsNotNone(conn.execute("SELECT id FROM users WHERE id = ?", (target["id"],)).fetchone())
            self.assertIsNotNone(conn.execute("SELECT id FROM products WHERE id = 'p-1'").fetchone())
            self.assertEqual(
                conn.execute("SELECT order_status FROM orders WHERE id = ?", (order["order_id"],)).fetchone()[0],
                "pending_confirmation",
            )

    def test_order_migration_is_idempotent_and_preserves_legacy_confirmed(self):
        created_at = datetime.utcnow().isoformat()
        with self.connect() as conn:
            for order_id, status in (("ord-legacy-processing", "processing"), ("ord-legacy-confirmed", "confirmed")):
                conn.execute(
                    """
                    INSERT INTO orders (
                        id, user_id, items_json, subtotal_vnd, total_vnd, created_at,
                        order_status, payment_method, payment_status
                    ) VALUES (?, 'u-demo', '[]', 0, 0, ?, ?, 'COD', 'unpaid')
                    """,
                    (order_id, created_at, status),
                )
        backend.init_db()
        backend.init_db()
        with self.connect() as conn:
            processing = conn.execute(
                "SELECT order_status, sim_started_at FROM orders WHERE id = 'ord-legacy-processing'"
            ).fetchone()
            confirmed = conn.execute(
                "SELECT order_status FROM orders WHERE id = 'ord-legacy-confirmed'"
            ).fetchone()
            history_count = conn.execute(
                "SELECT COUNT(*) FROM order_status_history WHERE order_id = 'ord-legacy-processing'"
            ).fetchone()[0]
        self.assertEqual(processing[0], "pending_confirmation")
        self.assertEqual(processing[1], created_at)
        self.assertEqual(confirmed[0], "confirmed")
        self.assertEqual(history_count, 1)

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

    def test_ai_stylist_uses_llm_and_limits_candidates_to_budget_and_owner(self):
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO wardrobe_items (
                    id, user_id, source_product_id, title, category, size, color,
                    material, original_price, paid_price, image_url, purchased_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "wardrobe-ai-anchor",
                    "u-demo",
                    "previously-owned-product",
                    "Quần linen beige",
                    "Quần",
                    "M",
                    "Be",
                    "Linen",
                    500000,
                    250000,
                    "https://example.test/wardrobe.jpg",
                    datetime.utcnow().isoformat(),
                ),
            )

            conn.execute(
                """
                INSERT INTO products (
                    id, seller_id, title, category, size, color, material,
                    condition_score, condition_label, ai_note, price, original_price,
                    cycle, points_reward, trend, fit_advice, image_url, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "p-ai-own-listing",
                    "u-demo",
                    "Áo tự đăng bán",
                    "Áo",
                    "M",
                    "Đen",
                    "Cotton",
                    95,
                    "Như mới",
                    "Sản phẩm kiểm thử",
                    100000,
                    300000,
                    1,
                    50,
                    "Minimalist Thanh Lịch",
                    "M",
                    "https://example.test/own-listing.jpg",
                    "available",
                    datetime.utcnow().isoformat(),
                ),
            )

        model_response = {
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "recommendations": [
                                    {
                                        "product_id": "p-1",
                                        "stylist_tip": (
                                            "Áo linen sáng màu tạo tổng thể cân bằng "
                                            "với quần beige."
                                        ),
                                    }
                                ]
                            },
                            ensure_ascii=False,
                        )
                    }
                }
            ]
        }
        with patch.object(
            backend,
            "call_vilao_chat_completion",
            return_value=model_response,
        ) as call_ai:
            result = backend.ai_stylist_recommendations(
                anchor_item_id="wardrobe-ai-anchor",
                trend="Công Sở Hiện Đại",
                occasion="Đi làm",
                max_price=300000,
                user=self.user,
            )

        self.assertEqual(result["source"], "ai")
        self.assertEqual(len(result["outfits"]), 1)
        self.assertEqual(
            result["outfits"][0]["recommended_market_item"]["id"],
            "p-1",
        )
        self.assertIn("quần beige", result["outfits"][0]["stylist_tip"])
        self.assertEqual(result["outfits"][0]["missing_item_price"], 290000)
        self.assertEqual(
            call_ai.call_args.kwargs["response_format"],
            {"type": "json_object"},
        )
        prompt_context = json.loads(call_ai.call_args.kwargs["messages"][1]["content"])
        self.assertEqual(prompt_context["preferences"]["occasion"], "Đi làm")
        self.assertEqual(prompt_context["preferences"]["max_price_vnd"], 300000)
        self.assertTrue(
            all(item["price_vnd"] <= 300000 for item in prompt_context["catalog"])
        )
        self.assertNotIn(
            "p-ai-own-listing",
            {item["id"] for item in prompt_context["catalog"]},
        )
        self.assertNotIn(self.user["email"], json.dumps(prompt_context))

    def test_ai_stylist_rejects_unowned_anchor_and_falls_back_on_invalid_model_output(self):
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO wardrobe_items (
                    id, user_id, source_product_id, title, category, size, color,
                    material, original_price, paid_price, image_url, purchased_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "wardrobe-fallback-anchor",
                    "u-demo",
                    None,
                    "Quần linen beige",
                    "Quần",
                    "M",
                    "Be",
                    "Linen",
                    500000,
                    250000,
                    "https://example.test/wardrobe.jpg",
                    datetime.utcnow().isoformat(),
                ),
            )

        with self.assertRaises(backend.HTTPException) as invalid_anchor:
            backend.ai_stylist_recommendations(
                anchor_item_id="another-users-wardrobe-item",
                trend="Minimalist Thanh Lịch",
                occasion=None,
                max_price=None,
                user=self.user,
            )
        self.assertEqual(invalid_anchor.exception.status_code, 404)

        with patch.object(
            backend,
            "call_vilao_chat_completion",
            return_value={
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "recommendations": [
                                        {
                                            "product_id": "not-in-the-live-catalog",
                                            "stylist_tip": "Sản phẩm tưởng tượng.",
                                        }
                                    ]
                                }
                            )
                        }
                    }
                ]
            },
        ):
            result = backend.ai_stylist_recommendations(
                anchor_item_id="wardrobe-fallback-anchor",
                trend="Minimalist Thanh Lịch",
                occasion=None,
                max_price=None,
                user=self.user,
            )

        self.assertEqual(result["source"], "rules_fallback")
        self.assertTrue(result["outfits"])
        self.assertTrue(
            all(
                outfit["recommended_market_item"]["id"].startswith("p-")
                for outfit in result["outfits"]
            )
        )

        with patch.object(
            backend,
            "call_vilao_chat_completion",
            side_effect=backend.HTTPException(
                status_code=502,
                detail="AI provider unavailable",
            ),
        ):
            unavailable_result = backend.ai_stylist_recommendations(
                anchor_item_id="wardrobe-fallback-anchor",
                trend="Minimalist Thanh Lịch",
                occasion=None,
                max_price=None,
                user=self.user,
            )

        self.assertEqual(unavailable_result["source"], "rules_fallback")
        self.assertTrue(unavailable_result["outfits"])

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

    def test_checkout_prepares_buyer_and_per_seller_email_details(self):
        seller_a = self.insert_user("seller-email-a", "seller-a@example.vn")
        seller_b = self.insert_user("seller-email-b", "seller-b@example.vn")
        with self.connect() as conn:
            conn.execute(
                "UPDATE products SET seller_id = ? WHERE id = 'p-1'",
                (seller_a["id"],),
            )
            conn.execute(
                "UPDATE products SET seller_id = ? WHERE id = 'p-4'",
                (seller_b["id"],),
            )

        payload = backend.CheckoutRequest(
            product_ids=["p-1", "p-4"],
            recipient_name="Người nhận thử",
            phone="0912345678",
            shipping_address="12 Nguyễn Huệ, Quận 1, TP HCM",
            payment_method="BANK_TRANSFER",
        )
        with patch.object(backend, "send_order_notification_emails") as send_emails:
            order = backend.checkout_order(payload, self.user)

        send_emails.assert_called_once()
        email_order = send_emails.call_args.args[0]
        self.assertEqual(email_order["order_id"], order["order_id"])
        self.assertEqual(email_order["buyer_email"], self.user["email"])
        self.assertEqual(email_order["recipient_name"], "Người nhận thử")
        self.assertEqual(email_order["phone"], "0912345678")
        self.assertEqual(email_order["payment_status"], "pending")
        self.assertEqual(len(email_order["items"]), 2)
        sellers = {seller["email"]: seller for seller in email_order["sellers"]}
        self.assertEqual(set(sellers), {"seller-a@example.vn", "seller-b@example.vn"})
        self.assertEqual(len(sellers["seller-a@example.vn"]["items"]), 1)
        self.assertEqual(len(sellers["seller-b@example.vn"]["items"]), 1)

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
        self.assertEqual(cod["order_status"], "pending_confirmation")
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

    def test_simulated_payment_delivery_confirmation_is_idempotent(self):
        order = backend.checkout_order(
            self.checkout_payload("p-1", method="BANK_TRANSFER"), self.user
        )
        with self.assertRaises(backend.HTTPException) as unauthorized:
            backend.pay_simulated_order(
                order["order_id"],
                backend.SimulatedPaymentRequest(outcome="success"),
                {"id": "u-other"},
            )
        self.assertEqual(unauthorized.exception.status_code, 403)

        failure = backend.pay_simulated_order(
            order["order_id"],
            backend.SimulatedPaymentRequest(outcome="failed"),
            self.user,
        )
        self.assertEqual(failure["order_status"], "pending_payment")
        payment = backend.pay_simulated_order(
            order["order_id"],
            backend.SimulatedPaymentRequest(outcome="success"),
            self.user,
        )
        self.assertEqual(payment["order_status"], "pending_confirmation")
        simulated_start = datetime.utcnow() - timedelta(
            seconds=backend.ORDER_SIM_STEP_SECONDS * 4 + 1
        )
        with self.connect() as conn:
            conn.execute(
                "UPDATE orders SET sim_started_at = ?, created_at = ? WHERE id = ?",
                (
                    simulated_start.isoformat(),
                    (simulated_start - timedelta(seconds=1)).isoformat(),
                    order["order_id"],
                ),
            )
            conn.execute(
                """
                UPDATE order_status_history SET created_at = ?
                WHERE order_id = ? AND status = 'pending_confirmation'
                """,
                (simulated_start.isoformat(), order["order_id"]),
            )
            conn.execute(
                """
                UPDATE order_status_history SET created_at = ?
                WHERE order_id = ? AND status = 'pending_payment'
                """,
                ((simulated_start - timedelta(seconds=1)).isoformat(), order["order_id"]),
            )
        result = backend.get_my_orders(self.user)
        current = next(item for item in result["items"] if item["order_id"] == order["order_id"])
        self.assertEqual(current["order_status"], "delivered")
        self.assertEqual(current["recipient_name"], "Nguyễn An")
        self.assertEqual(current["phone"], "0912345678")
        self.assertEqual(current["shipping_address"], "12 Nguyễn Huệ, Quận 1")
        self.assertEqual(current["tracking_code"][:3], "RLX")
        self.assertEqual(
            [entry["status"] for entry in current["timeline"]],
            ["pending_payment", "pending_confirmation", "preparing", "shipping", "out_for_delivery", "delivered"],
        )

        backend.confirm_order_received(order["order_id"], self.user)
        backend.confirm_order_received(order["order_id"], self.user)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT order_status FROM orders WHERE id = ?", (order["order_id"],)).fetchone()[0],
                "completed",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM wardrobe_items WHERE source_product_id = 'p-1'").fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM payments WHERE order_id = ? AND status IN ('failed', 'success')",
                    (order["order_id"],),
                ).fetchone()[0],
                2,
            )

    def test_buyer_cannot_confirm_undelivered_order_and_can_cancel(self):
        order = backend.checkout_order(self.checkout_payload("p-1"), self.user)
        with self.assertRaises(backend.HTTPException) as early:
            backend.confirm_order_received(order["order_id"], self.user)
        self.assertEqual(early.exception.status_code, 409)
        with self.assertRaises(backend.HTTPException) as unauthorized:
            backend.cancel_order(order["order_id"], {"id": "u-other"})
        self.assertEqual(unauthorized.exception.status_code, 403)
        backend.cancel_order(order["order_id"], self.user)
        with self.connect() as conn:
            self.assertEqual(
                conn.execute("SELECT status FROM products WHERE id = 'p-1'").fetchone()[0],
                "available",
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM product_reservations WHERE order_id = ?", (order["order_id"],)).fetchone()[0],
                0,
            )

    def test_complaint_window_starts_at_completion(self):
        order = backend.checkout_order(self.checkout_payload("p-1"), self.user)
        complaint_payload = backend.CreateComplaintRequest(
            order_id=order["order_id"],
            product_id="p-1",
            reason_code="condition_worse",
            description="Tình trạng sản phẩm khác với mô tả ban đầu.",
            desired_resolution="exchange",
            evidence_images=["data:image/png;base64,iVBORw0KGgo="],
        )
        with self.assertRaises(backend.HTTPException) as before_completion:
            backend.create_complaint(complaint_payload, self.user)
        self.assertEqual(before_completion.exception.status_code, 409)

        backend.complete_admin_cod_order(order["order_id"], self.admin)
        completed_at = datetime.utcnow().isoformat()
        with self.connect() as conn:
            conn.execute(
                "UPDATE orders SET created_at = ?, completed_at = ? WHERE id = ?",
                (
                    (datetime.utcnow() - timedelta(days=30)).isoformat(),
                    completed_at,
                    order["order_id"],
                ),
            )
        purchased = backend.get_my_orders(self.user)["items"][0]
        self.assertTrue(purchased["items"][0]["can_complain"])

        with self.connect() as conn:
            conn.execute(
                "UPDATE orders SET completed_at = ? WHERE id = ?",
                (
                    (datetime.utcnow() - timedelta(days=8)).isoformat(),
                    order["order_id"],
                ),
            )
        purchased = backend.get_my_orders(self.user)["items"][0]
        self.assertFalse(purchased["items"][0]["can_complain"])
        with self.assertRaises(backend.HTTPException) as expired_window:
            backend.create_complaint(complaint_payload, self.user)
        self.assertEqual(expired_window.exception.status_code, 400)

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

    def test_welcome_email_uses_published_resend_template_alias(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        with patch.dict(
            os.environ,
            {
                "RESEND_API_KEY": "test-resend-key",
                "RESEND_FROM_EMAIL": "RE:LOOP <no-reply@reloop.help>",
                "RESEND_WELCOME_TEMPLATE_ID": "th-cho-mng",
            },
        ), patch.object(
            backend.urllib.request, "urlopen", return_value=FakeResponse()
        ) as urlopen:
            sent, notice = backend.send_welcome_email("Nguyễn An", "qa@example.com")

        self.assertTrue(sent)
        self.assertIsNone(notice)
        request = urlopen.call_args.args[0]
        payload = json.loads(request.data)
        self.assertEqual(request.full_url, "https://api.resend.com/emails")
        self.assertEqual(request.get_header("User-agent"), "reloop/1.0")
        self.assertEqual(payload["template"], {"id": "th-cho-mng", "variables": {}})
        self.assertNotIn("html", payload)
        self.assertNotIn("text", payload)

    def test_verification_email_contains_public_confirmation_link(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        with patch.dict(
            os.environ,
            {
                "RESEND_API_KEY": "test-resend-key",
                "RESEND_FROM_EMAIL": "RE:LOOP <no-reply@reloop.help>",
            },
        ), patch.object(
            backend, "RELOOP_PUBLIC_API_URL", "https://api.example.test"
        ), patch.object(
            backend.urllib.request, "urlopen", return_value=FakeResponse()
        ) as urlopen:
            sent, notice = backend.send_email_verification(
                "Nguyễn An",
                "qa@example.com",
                "opaque-verification-token",
            )

        self.assertTrue(sent)
        self.assertIsNone(notice)
        payload = json.loads(urlopen.call_args.args[0].data)
        verification_url = (
            "https://api.example.test/api/auth/verify-email"
            "?token=opaque-verification-token"
        )
        self.assertEqual(payload["to"], ["qa@example.com"])
        self.assertIn("Chào mừng bạn đến với RE:LOOP", payload["subject"])
        self.assertIn("background-color:#123d31", payload["html"])
        self.assertIn("XÁC MINH EMAIL", payload["html"])
        self.assertIn(verification_url, payload["text"])
        self.assertIn(verification_url, payload["html"])
        self.assertIn("24 giờ", payload["text"])

    def test_verification_email_escapes_name_in_html(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        with patch.dict(
            os.environ,
            {
                "RESEND_API_KEY": "test-resend-key",
                "RESEND_FROM_EMAIL": "RE:LOOP <no-reply@reloop.help>",
            },
        ), patch.object(
            backend.urllib.request, "urlopen", return_value=FakeResponse()
        ) as urlopen:
            backend.send_email_verification(
                "<script>alert(1)</script>",
                "qa@example.com",
                "opaque-verification-token",
            )

        payload = json.loads(urlopen.call_args.args[0].data)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", payload["html"])
        self.assertNotIn("<script>", payload["html"])

    def test_order_emails_include_seller_buyer_and_recipient_details(self):
        order = {
            "order_id": "ord-email-test",
            "buyer_name": "Người mua",
            "buyer_email": "buyer@example.vn",
            "recipient_name": "Người nhận",
            "phone": "0912345678",
            "shipping_address": "12 Nguyễn Huệ, Quận 1",
            "items": [
                {
                    "title": "Áo linen",
                    "size": "M",
                    "color": "Trắng",
                    "price": 290000,
                    "seller_name": "Người bán A",
                    "seller_email": "seller-a@example.vn",
                }
            ],
            "sellers": [
                {
                    "name": "Người bán A",
                    "email": "seller-a@example.vn",
                    "items": [
                        {
                            "title": "Áo linen",
                            "size": "M",
                            "color": "Trắng",
                            "price": 290000,
                        }
                    ],
                }
            ],
            "subtotal_vnd": 290000,
            "discount_vnd": 10000,
            "total_vnd": 280000,
            "payment_label": "Chuyển khoản",
            "payment_status": "pending",
            "status_label": "Chờ thanh toán",
        }
        with patch.object(
            backend,
            "_send_resend_email",
            return_value=(True, None),
        ) as send_resend:
            backend.send_buyer_order_confirmation(order)
            buyer_email = send_resend.call_args.args[0]
            self.assertEqual(buyer_email["to"], ["buyer@example.vn"])
            self.assertIn("seller-a@example.vn", buyer_email["html"])
            self.assertIn("Người nhận", buyer_email["html"])
            self.assertIn("12 Nguyễn Huệ", buyer_email["html"])
            self.assertIn("280.000 ₫", buyer_email["html"])
            self.assertIn("seller-a@example.vn", buyer_email["text"])

            backend.send_seller_order_notification(order, order["sellers"][0])
            seller_email = send_resend.call_args.args[0]
            self.assertEqual(seller_email["to"], ["seller-a@example.vn"])
            self.assertIn("buyer@example.vn", seller_email["html"])
            self.assertIn("Người nhận", seller_email["html"])
            self.assertIn("12 Nguyễn Huệ", seller_email["html"])
            self.assertIn("chờ thanh toán", seller_email["text"].lower())

    def test_welcome_email_logs_resend_error_details_without_secrets(self):
        response_body = json.dumps({
            "type": "validation_error",
            "error": "Sending to qa@example.com failed for key re_testSecret123",
        }).encode("utf-8")
        resend_error = backend.urllib.error.HTTPError(
            "https://api.resend.com/emails",
            403,
            "Forbidden",
            hdrs=None,
            fp=io.BytesIO(response_body),
        )

        with patch.dict(
            os.environ,
            {
                "RESEND_API_KEY": "re_testSecret123",
                "RESEND_FROM_EMAIL": "RE:LOOP <no-reply@reloop.help>",
            },
            clear=False,
        ), patch.object(
            backend.urllib.request, "urlopen", side_effect=resend_error
        ), self.assertLogs(backend.logger, level="ERROR") as captured:
            sent, notice = backend.send_welcome_email("Nguyễn An", "qa@example.com")

        self.assertFalse(sent)
        self.assertIsNotNone(notice)
        log_output = "\n".join(captured.output)
        self.assertIn("HTTP 403 (validation_error)", log_output)
        self.assertIn("[email]", log_output)
        self.assertIn("[REDACTED_API_KEY]", log_output)
        self.assertNotIn("qa@example.com", log_output)
        self.assertNotIn("re_testSecret123", log_output)

    def test_welcome_email_logs_plain_text_resend_errors(self):
        resend_error = backend.urllib.error.HTTPError(
            "https://api.resend.com/emails",
            403,
            "Forbidden",
            hdrs=None,
            fp=io.BytesIO(b"error code: 1010 Access denied"),
        )

        with patch.dict(
            os.environ,
            {
                "RESEND_API_KEY": "test-resend-key",
                "RESEND_FROM_EMAIL": "RE:LOOP <no-reply@reloop.help>",
            },
            clear=False,
        ), patch.object(
            backend.urllib.request, "urlopen", side_effect=resend_error
        ), self.assertLogs(backend.logger, level="ERROR") as captured:
            sent, _ = backend.send_welcome_email("Nguyễn An", "qa@example.com")

        self.assertFalse(sent)
        self.assertIn("HTTP 403 (non_json_error): error code: 1010 Access denied", captured.output[0])

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

            verification_record = logging.LogRecord(
                logger_name,
                logging.INFO,
                __file__,
                1,
                '%s - "GET %s" 200',
                (
                    "127.0.0.1:1234",
                    "/api/auth/verify-email?token=QA_EMAIL_TOKEN_SENTINEL",
                ),
                None,
            )
            backend.WebSocketTicketLogFilter().filter(verification_record)
            self.assertNotIn("QA_EMAIL_TOKEN_SENTINEL", verification_record.getMessage())
            self.assertIn("[REDACTED]", verification_record.getMessage())

            reset_record = logging.LogRecord(
                logger_name,
                logging.INFO,
                __file__,
                1,
                '%s - "GET %s" 200',
                (
                    "127.0.0.1:1234",
                    "/api/auth/reset-password?token=QA_RESET_TOKEN_SENTINEL",
                ),
                None,
            )
            backend.WebSocketTicketLogFilter().filter(reset_record)
            self.assertNotIn("QA_RESET_TOKEN_SENTINEL", reset_record.getMessage())
            self.assertIn("[REDACTED]", reset_record.getMessage())

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

    def test_email_verification_is_required_before_login_and_session_tokens_are_hashed(self):
        verification_token = "test-email-verification-token"
        with patch.object(backend.secrets, "token_urlsafe", return_value=verification_token), patch.object(
            backend, "send_email_verification", return_value=(True, None)
        ), patch.object(backend, "send_welcome_email", return_value=(True, None)) as welcome_email:
            response = backend.register_user(
                backend.RegisterRequest(
                    name="QA Session User",
                    email="qa-session@example.vn",
                    password="test-password-2026",
                )
            )

            self.assertTrue(response["email_sent"])
            self.assertNotIn("access_token", response)
            with self.connect() as conn:
                user_id = conn.execute(
                    "SELECT id FROM users WHERE email = ?",
                    ("qa-session@example.vn",),
                ).fetchone()[0]
                user_row = conn.execute(
                    "SELECT email_verified FROM users WHERE id = ?", (user_id,)
                ).fetchone()
                verification_hash = conn.execute(
                    "SELECT token_hash FROM email_verification_tokens WHERE user_id = ?",
                    (user_id,),
                ).fetchone()[0]
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM sessions WHERE user_id = ?", (user_id,)
                    ).fetchone()[0],
                    0,
                )
            self.assertEqual(user_row[0], 0)
            self.assertEqual(
                verification_hash,
                backend.hashlib.sha256(verification_token.encode("utf-8")).hexdigest(),
            )
            self.assertNotEqual(verification_hash, verification_token)

            with self.assertRaises(backend.HTTPException) as unverified_login:
                backend.login_user(
                    backend.LoginRequest(
                        email="qa-session@example.vn",
                        password="test-password-2026",
                    )
                )
            self.assertEqual(unverified_login.exception.status_code, 403)
            self.assertIn("Gmail/Spam", unverified_login.exception.detail)

            page = backend.show_email_verification(token=verification_token)
            self.assertEqual(page.status_code, 200)
            self.assertIn(b"X\xc3\xa1c nh\xe1\xba\xadn", page.body)
            with self.connect() as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT email_verified FROM users WHERE id = ?", (user_id,)
                    ).fetchone()[0],
                    0,
                )

            confirmation = backend.confirm_email_verification(token=verification_token)
            self.assertEqual(confirmation.status_code, 200)
            welcome_email.assert_called_once_with("QA Session User", "qa-session@example.vn")
            login_response = backend.login_user(
                backend.LoginRequest(
                    email="qa-session@example.vn",
                    password="test-password-2026",
                )
            )

        token = login_response["access_token"]
        hashed_token = backend.hash_session_token(token)

        with self.connect() as conn:
            stored_token = conn.execute(
                "SELECT token FROM sessions WHERE user_id = ?", (login_response["user"]["id"],)
            ).fetchone()[0]
        self.assertEqual(stored_token, hashed_token)
        self.assertNotEqual(stored_token, token)
        backend.get_current_user(f"Bearer {token}")

        backend.logout_user(f"Bearer {token}")
        with self.assertRaises(backend.HTTPException) as error:
            backend.get_current_user(f"Bearer {token}")
        self.assertEqual(error.exception.status_code, 401)

    def test_email_verification_resend_is_throttled_and_expired_links_are_rejected(self):
        verification_token = "test-expiring-verification-token"
        with patch.object(backend.secrets, "token_urlsafe", return_value=verification_token), patch.object(
            backend, "send_email_verification", return_value=(True, None)
        ) as send_verification, patch.object(
            backend, "send_welcome_email", return_value=(True, None)
        ):
            backend.register_user(
                backend.RegisterRequest(
                    name="QA Pending User",
                    email="qa-pending@example.vn",
                    password="test-password-2026",
                )
            )
            generic_response = backend.resend_email_verification(
                backend.ResendVerificationRequest(email="unknown@example.vn")
            )
            cooldown_response = backend.resend_email_verification(
                backend.ResendVerificationRequest(email="qa-pending@example.vn")
            )
            self.assertEqual(generic_response, cooldown_response)
            send_verification.assert_called_once()

            with self.connect() as conn:
                conn.execute(
                    """
                    UPDATE email_verification_tokens SET expires_at = ?
                    WHERE user_id = (
                        SELECT id FROM users WHERE email = 'qa-pending@example.vn'
                    )
                    """,
                    ((datetime.utcnow() - timedelta(seconds=1)).isoformat(),),
                )
            page = backend.show_email_verification(token=verification_token)
            self.assertEqual(page.status_code, 400)
            failed_confirmation = backend.confirm_email_verification(token=verification_token)
            self.assertEqual(failed_confirmation.status_code, 400)
            with self.connect() as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT email_verified FROM users WHERE email = ?",
                        ("qa-pending@example.vn",),
                    ).fetchone()[0],
                    0,
                )

    def test_failed_verification_delivery_can_be_retried_immediately(self):
        verification_token = "test-retry-verification-token"
        with patch.object(backend.secrets, "token_urlsafe", return_value=verification_token), patch.object(
            backend,
            "send_email_verification",
            side_effect=[(False, "Resend unavailable"), (True, None)],
        ) as send_verification:
            backend.register_user(
                backend.RegisterRequest(
                    name="QA Retry User",
                    email="qa-retry@example.vn",
                    password="test-password-2026",
                )
            )
            response = backend.resend_email_verification(
                backend.ResendVerificationRequest(email="qa-retry@example.vn")
            )

        self.assertIn("chúng tôi sẽ gửi email", response["message"])
        self.assertEqual(send_verification.call_count, 2)

    def test_password_reset_email_is_branded_and_escapes_user_content(self):
        with patch.object(
            backend,
            "_send_resend_email",
            return_value=(True, None),
        ) as send_resend:
            result = backend.send_password_reset_email(
                "<QA User>",
                "qa-reset@example.vn",
                "qa-reset-token-1234567890",
            )

        self.assertEqual(result, (True, None))
        payload = send_resend.call_args.args[0]
        self.assertEqual(payload["to"], ["qa-reset@example.vn"])
        self.assertIn("Đặt lại mật khẩu", payload["subject"])
        self.assertIn("RE:LOOP", payload["html"])
        self.assertIn("TẠO MẬT KHẨU MỚI", payload["html"])
        self.assertIn("&lt;QA User&gt;", payload["html"])
        self.assertNotIn("<QA User>", payload["html"])
        self.assertIn("qa-reset-token-1234567890", payload["text"])

    def test_password_reset_is_generic_throttled_single_use_and_revokes_sessions(self):
        old_password = "old-test-password"
        new_password = "new-test-password"
        password_hash, salt = backend.hash_password(old_password)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (
                    id, name, email, password_hash, salt, created_at, email_verified
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    "u-password-reset",
                    "QA Reset User",
                    "qa-reset@example.vn",
                    password_hash,
                    salt,
                    datetime.utcnow().isoformat(),
                ),
            )

        reset_token = "qa-password-reset-token-1234567890"
        with patch.object(
            backend.secrets,
            "token_urlsafe",
            return_value=reset_token,
        ), patch.object(
            backend,
            "send_password_reset_email",
            return_value=(True, None),
        ) as send_reset_email:
            response = backend.request_password_reset(
                backend.PasswordResetRequest(email="QA-RESET@example.vn")
            )
            repeated_response = backend.request_password_reset(
                backend.PasswordResetRequest(email="qa-reset@example.vn")
            )
            unknown_response = backend.request_password_reset(
                backend.PasswordResetRequest(email="unknown-reset@example.vn")
            )

        self.assertEqual(response, repeated_response)
        self.assertEqual(response, unknown_response)
        self.assertEqual(send_reset_email.call_count, 1)
        send_reset_email.assert_called_once_with(
            "QA Reset User",
            "qa-reset@example.vn",
            reset_token,
        )
        with self.connect() as conn:
            reset_row = conn.execute(
                """
                SELECT token_hash FROM password_reset_tokens
                WHERE user_id = 'u-password-reset'
                """
            ).fetchone()
            conn.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                (
                    "session-to-revoke",
                    "u-password-reset",
                    (datetime.utcnow() + timedelta(days=1)).isoformat(),
                ),
            )
        self.assertEqual(
            reset_row[0],
            backend.hashlib.sha256(reset_token.encode("utf-8")).hexdigest(),
        )
        self.assertNotEqual(reset_row[0], reset_token)

        page = backend.show_password_reset(token=reset_token)
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"new_password", page.body)
        self.assertEqual(page.headers["Referrer-Policy"], "no-referrer")

        mismatch = backend.confirm_password_reset(
            token=reset_token,
            new_password=new_password,
            confirm_password="different-password",
        )
        self.assertEqual(mismatch.status_code, 400)
        self.assertTrue(
            backend.verify_password(
                old_password,
                password_hash,
                salt,
            )
        )

        confirmation = backend.confirm_password_reset(
            token=reset_token,
            new_password=new_password,
            confirm_password=new_password,
        )
        self.assertEqual(confirmation.status_code, 200)
        self.assertIn(b"M\xe1\xba\xadt kh\xe1\xba\xa9u", confirmation.body)
        with self.connect() as conn:
            updated = conn.execute(
                "SELECT password_hash, salt FROM users WHERE id = 'u-password-reset'"
            ).fetchone()
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sessions WHERE user_id = 'u-password-reset'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM password_reset_tokens WHERE user_id = 'u-password-reset'"
                ).fetchone()[0],
                0,
            )
        self.assertTrue(backend.verify_password(new_password, updated[0], updated[1]))
        self.assertFalse(backend.verify_password(old_password, updated[0], updated[1]))
        reused = backend.confirm_password_reset(
            token=reset_token,
            new_password="another-password",
            confirm_password="another-password",
        )
        self.assertEqual(reused.status_code, 400)

    def test_expired_password_reset_links_are_rejected(self):
        password_hash, salt = backend.hash_password("keep-this-password")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (
                    id, name, email, password_hash, salt, created_at, email_verified
                ) VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    "u-expired-reset",
                    "QA Expired User",
                    "qa-expired-reset@example.vn",
                    password_hash,
                    salt,
                    datetime.utcnow().isoformat(),
                ),
            )

        reset_token = "qa-expired-password-reset-token"
        with patch.object(
            backend.secrets,
            "token_urlsafe",
            return_value=reset_token,
        ), patch.object(
            backend,
            "send_password_reset_email",
            return_value=(True, None),
        ):
            backend.request_password_reset(
                backend.PasswordResetRequest(email="qa-expired-reset@example.vn")
            )
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE password_reset_tokens SET expires_at = ?
                WHERE user_id = 'u-expired-reset'
                """,
                ((datetime.utcnow() - timedelta(seconds=1)).isoformat(),),
            )

        page = backend.show_password_reset(token=reset_token)
        self.assertEqual(page.status_code, 400)
        response = backend.confirm_password_reset(
            token=reset_token,
            new_password="new-password-123",
            confirm_password="new-password-123",
        )
        self.assertEqual(response.status_code, 400)
        self.assertTrue(backend.verify_password("keep-this-password", password_hash, salt))

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
