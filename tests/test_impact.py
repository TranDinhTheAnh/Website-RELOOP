import json
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import reloop_backend as backend


@pytest.fixture
def impact_client(tmp_path, monkeypatch):
    monkeypatch.setattr(backend, "DATABASE_URL", "")
    monkeypatch.setattr(backend, "DATABASE_URL_UNPOOLED", "")
    monkeypatch.setattr(backend, "_postgres_pool", None)
    monkeypatch.setattr(backend, "DB_PATH", str(tmp_path / "impact-test.db"))
    backend._impact_platform_cache.update(
        {"database": None, "expires_at": 0.0, "data": None}
    )
    backend.init_db()
    users = {}
    with backend.get_db() as conn:
        cursor = conn.cursor()
        for user_id in ("impact-a", "impact-b"):
            email = f"{user_id}@example.test"
            cursor.execute(
                """
                INSERT INTO users (
                    id, name, email, password_hash, salt, green_points,
                    co2_saved, water_saved, donated_vnd, created_at, email_verified
                ) VALUES (?, ?, ?, '', '', 0, 0, 0, 0, ?, 1)
                """,
                (user_id, user_id, email, datetime.utcnow().isoformat()),
            )
            token = f"token-{user_id}"
            cursor.execute(
                "INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)",
                (
                    backend.hash_session_token(token),
                    user_id,
                    (datetime.utcnow() + timedelta(hours=1)).isoformat(),
                ),
            )
            users[user_id] = token
    with TestClient(backend.app) as client:
        yield client, users


def insert_product(product_id, category, cycle=1, status="available"):
    with backend.get_db() as conn:
        conn.cursor().execute(
            """
            INSERT INTO products (
                id, seller_id, title, category, size, color, material,
                condition_score, condition_label, ai_note, price, original_price,
                cycle, points_reward, trend, fit_advice, image_url, status, created_at
            ) VALUES (?, NULL, ?, ?, 'M', 'Đen', 'Cotton', 90, 'Tốt', 'Kiểm thử',
                      100000, 150000, ?, 70, 'Basic', 'Kiểm thử',
                      'https://example.test/item.jpg', ?, ?)
            """,
            (
                product_id,
                f"Sản phẩm {product_id}",
                category,
                cycle,
                status,
                datetime.utcnow().isoformat(),
            ),
        )


def insert_order(order_id, user_id, product_ids, status="completed"):
    with backend.get_db() as conn:
        conn.cursor().execute(
            """
            INSERT INTO orders (
                id, user_id, items_json, subtotal_vnd, total_vnd,
                created_at, order_status, completed_at, recipient_name,
                phone, shipping_address, payment_method, payment_status
            ) VALUES (?, ?, ?, 100000, 100000, ?, ?, ?, 'Người mua thử',
                      '0912345678', 'Địa chỉ thử', 'COD', 'paid')
            """,
            (
                order_id,
                user_id,
                json.dumps(product_ids),
                datetime.utcnow().isoformat(),
                status,
                datetime.utcnow().isoformat() if status == "completed" else None,
            ),
        )


def insert_complaint(complaint_id, order_id, product_id, resolution_type):
    now = datetime.utcnow().isoformat()
    with backend.get_db() as conn:
        conn.cursor().execute(
            """
            INSERT INTO complaints (
                id, order_id, product_id, buyer_id, seller_id, reason_code,
                description, desired_resolution, evidence_json, product_snapshot_json,
                status, resolution_type, created_at, updated_at, resolved_at
            ) VALUES (?, ?, ?, 'impact-a', NULL, 'other', 'Mô tả kiểm thử',
                      'refund', '[]', '{}', 'resolved', ?, ?, ?, ?)
            """,
            (complaint_id, order_id, product_id, resolution_type, now, now, now),
        )


def get_summary(client, token):
    response = client.get(
        "/api/impact/me", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200
    return response.json()


def test_empty_summary_and_single_reference_category_formula(impact_client):
    client, users = impact_client
    empty = get_summary(client, users["impact-a"])
    assert empty["items_reused"] == 0
    assert empty["is_estimate"] is True
    assert empty["methodology_version"] == backend.IMPACT_METHODOLOGY["methodology_version"]
    assert empty["co2e_avoided_kg"] == 0.0
    assert empty["water_avoided_l"] == 0.0

    categories = list(backend.IMPACT_METHODOLOGY["category_mass_kg"])
    product_ids = []
    for index, category in enumerate(categories):
        product_id = f"formula-{index}"
        product_ids.append(product_id)
        insert_product(product_id, category)
    insert_order("formula-order", "impact-a", product_ids + [product_ids[0]])

    summary = get_summary(client, users["impact-a"])
    assert summary["items_reused"] == len(categories)
    assert len(summary["by_category"]) == len(categories)
    for item in summary["by_category"]:
        category = item["category"]
        mass = backend.IMPACT_METHODOLOGY["category_mass_kg"][category]
        reference = backend.IMPACT_METHODOLOGY["reference_assumptions"]
        expected_co2 = max(
            0,
            mass
            * backend.IMPACT_METHODOLOGY["production_impact_per_kg"]["co2e_kg"]
            * reference["replacement_rate"]
            - reference["additional_co2e_kg"],
        )
        expected_water = max(
            0,
            mass
            * backend.IMPACT_METHODOLOGY["production_impact_per_kg"]["water_l"]
            * reference["replacement_rate"]
            - reference["additional_water_l"],
        )
        assert item["co2e_avoided_kg"] == pytest.approx(expected_co2)
        assert item["water_avoided_l"] == pytest.approx(expected_water)
        assert isinstance(item["co2e_avoided_kg"], float)
        assert isinstance(item["water_avoided_l"], float)

    for metric in ("co2e_avoided_kg", "water_avoided_l"):
        values = summary[metric]
        assert isinstance(values, float)
        assert values >= 0 and values == values
        assert summary[metric] == pytest.approx(
            sum(item[metric] for item in summary["by_category"])
        )


def test_cancelled_pending_and_accepted_complaint_refunds_are_excluded(impact_client):
    client, users = impact_client
    product_ids = [
        "valid-item",
        "cancelled-item",
        "delivered-item",
        "refund-item",
        "partial-refund-item",
        "exchange-item",
    ]
    for product_id in product_ids:
        insert_product(product_id, "Áo")

    insert_order("valid-order", "impact-a", [product_ids[0], product_ids[0]])
    insert_order("cancelled-order", "impact-a", [product_ids[1]], "cancelled")
    insert_order("delivered-order", "impact-a", [product_ids[2]], "delivered")
    insert_order("refund-order", "impact-a", [product_ids[3]])
    insert_order("partial-refund-order", "impact-a", [product_ids[4]])
    insert_order("exchange-order", "impact-a", [product_ids[5]])
    insert_complaint("refund-case", "refund-order", product_ids[3], "refund")
    insert_complaint(
        "partial-refund-case",
        "partial-refund-order",
        product_ids[4],
        "partial_refund",
    )
    insert_complaint("exchange-case", "exchange-order", product_ids[5], "exchange")

    summary = get_summary(client, users["impact-a"])
    assert summary["items_reused"] == 1
    assert summary["by_category"][0]["category"] == "Áo"


def test_resale_counts_only_after_second_sale_and_completion_is_idempotent(impact_client):
    client, users = impact_client
    insert_product("first-sale", "Áo", cycle=1, status="sold")
    insert_order("first-order", "impact-a", ["first-sale"])
    insert_product("resale-listing", "Áo khoác", cycle=2, status="reserved")

    # A reconsigned listing without a completed order is not a reuse event.
    assert get_summary(client, users["impact-a"])["items_reused"] == 1

    insert_order("second-order", "impact-a", ["resale-listing"], "delivered")
    with backend.get_db() as conn:
        conn.cursor().execute(
            """
            INSERT INTO product_reservations (product_id, order_id, expires_at)
            VALUES ('resale-listing', 'second-order', NULL)
            """
        )

    headers = {"Authorization": f"Bearer {users['impact-a']}"}
    first = client.post("/api/orders/second-order/confirm-received", headers=headers)
    second = client.post("/api/orders/second-order/confirm-received", headers=headers)
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["message"] == "Đơn hàng đã hoàn tất trước đó."
    assert get_summary(client, users["impact-a"])["items_reused"] == 2
    with backend.get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT green_points FROM users WHERE id = 'impact-a'")
        assert cursor.fetchone()["green_points"] == 70


def test_impact_api_auth_privacy_and_public_methodology(impact_client):
    client, users = impact_client
    insert_product("private-a", "Quần")
    insert_product("private-b", "Váy")
    insert_order("private-order-a", "impact-a", ["private-a"])

    summary_a = get_summary(client, users["impact-a"])
    one_user_platform = client.get("/api/impact/platform").json()
    assert one_user_platform["items_reused"] == summary_a["items_reused"] == 1
    assert one_user_platform["co2e_avoided_kg"] == summary_a["co2e_avoided_kg"]

    insert_order("private-order-b", "impact-b", ["private-b"])
    backend._impact_platform_cache["expires_at"] = 0

    assert client.get("/api/impact/me").status_code == 401
    summary_a = get_summary(client, users["impact-a"])
    summary_b = get_summary(client, users["impact-b"])
    assert summary_a["items_reused"] == 1
    assert summary_a["by_category"][0]["category"] == "Quần"
    assert summary_b["items_reused"] == 1
    assert summary_b["by_category"][0]["category"] == "Váy"

    platform_response = client.get("/api/impact/platform")
    methodology_response = client.get("/api/impact/methodology")
    assert platform_response.status_code == methodology_response.status_code == 200
    platform = platform_response.json()
    methodology = methodology_response.json()
    assert platform["items_reused"] == 2
    assert platform["items_reused"] == summary_a["items_reused"] + summary_b["items_reused"]
    assert platform_response.headers["cache-control"].startswith("public, max-age=")
    assert methodology["methodology_version"]
    assert methodology["is_estimate"] is True
    assert methodology["reference_assumptions"]["replacement_rate"] == 0.5
    assert "scenarios" not in methodology
    assert methodology["sources"]
    assert "impact-a@example.test" not in json.dumps(platform)
    assert "impact-b@example.test" not in json.dumps(platform)
    assert "impact-a@example.test" not in json.dumps(methodology)

    me_response = client.get(
        "/api/auth/me", headers={"Authorization": f"Bearer {users['impact-a']}"}
    )
    assert me_response.status_code == 200
    assert me_response.json()["user"]["co2_saved"] == pytest.approx(
        summary_a["co2e_avoided_kg"]
    )
    assert me_response.json()["user"]["water_saved"] == pytest.approx(
        summary_a["water_avoided_l"]
    )
