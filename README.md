# RE:LOOP

## Chạy backend

Cài các gói trong `requirements.txt`, cấu hình môi trường rồi chạy:

```powershell
$env:ADMIN_EMAIL = "admin@example.vn"
$env:ADMIN_PASSWORD = "<mật khẩu quản trị riêng, tối thiểu 12 ký tự>"
uvicorn reloop_backend:app --port 8000
```

`ADMIN_EMAIL` và `ADMIN_PASSWORD` đều cần được cấu hình để bật tài khoản quản trị. Không có thông tin mặc định trong mã nguồn; nếu cấu hình thiếu hoặc mật khẩu ngắn hơn 12 ký tự, backend không tạo tài khoản admin. Khi email đã tồn tại trong cơ sở dữ liệu, backend chỉ đổi mật khẩu nếu mật khẩu hiện tại không khớp cấu hình và thu hồi các phiên cũ.

Tài khoản Minh Anh chỉ còn là dữ liệu mẫu, không thể đăng nhập. Session token được lưu dưới dạng hash; token cũ được chuyển đổi khi khởi động, và endpoint đăng xuất thu hồi phiên hiện tại. Backend giới hạn 10 lần đăng nhập sai cho mỗi email trong 15 phút. Các kết nối SQLite bật kiểm tra khóa ngoại.

`RELOOP_CORS_ORIGINS` là danh sách origin được phép, phân cách bằng dấu phẩy. Mặc định gồm website Render và `localhost`/`127.0.0.1:8080`; thêm origin frontend tùy chỉnh vào biến môi trường Render nếu cần. CORS không bật cookie credentials.

Vé WebSocket được gửi qua subprotocol thay vì query string để tránh lộ trong access log; endpoint cũng từ chối vé đã dùng hoặc hết hạn.

## Neon và Render

Ứng dụng dùng custom auth hiện tại; Neon Auth đã được provision nhưng không dùng để đăng nhập. Để dùng Neon Postgres, cấu hình các biến môi trường sau trong Render Web Service:

- `DATABASE_URL`: pooled connection string của Neon, dùng cho truy vấn runtime.
- `DATABASE_URL_UNPOOLED`: direct connection string, dùng lúc khởi động để khởi tạo/cập nhật schema.
- `ADMIN_EMAIL` và `ADMIN_PASSWORD`: thông tin tài khoản quản trị riêng.
- `RESEND_API_KEY` và `RESEND_FROM_EMAIL`: cấu hình gửi email chào mừng qua Resend.
- `RESEND_WELCOME_TEMPLATE_ID`: tùy chọn; ID hoặc alias của template Resend đã publish. Nếu bỏ trống, backend dùng nội dung email chào mừng mặc định.

Không commit `.env.local`; dùng giá trị tương ứng từ `neon env pull`/Neon Console làm biến secret trên Render. Có thể đặt `RELOOP_DB_PATH` để chọn tệp SQLite khi chạy local mà không cấu hình `DATABASE_URL`.

Neon AI Gateway được ưu tiên khi có cả `NEON_AI_GATEWAY_TOKEN` và `NEON_AI_GATEWAY_BASE_URL`; model mặc định là `gpt-5-4-mini`, có thể đổi bằng `NEON_AI_MODEL`. Nếu không có cấu hình Neon, backend tiếp tục dùng `VILAO_API_KEY` hoặc `KIMI_API_KEY`. AI Gateway cần credits và quyền truy cập model.

Bucket private `uploads` nhận ảnh dạng base64 khi đăng sản phẩm/ký gửi lại; backend lưu object key trong Postgres và trả presigned read URL có hạn 7 ngày. Ảnh cũ và URL ảnh ngoài tiếp tục hoạt động, không tự động migrate. Cần cấu hình đủ `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL_S3` và `AWS_REGION` từ Neon trên Render.

Neon Realtime đã được provision nhưng tin nhắn vẫn dùng WebSocket FastAPI hiện có; Neon Realtime live queries chưa được tích hợp vào UI. Neon Auth cũng chưa được nối vào đăng nhập theo lựa chọn giữ custom auth.

## Build và kiểm thử

```powershell
npm run build
python -m py_compile reloop_backend.py
python -m unittest discover -s tests -v
```
