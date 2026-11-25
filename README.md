# RE:LOOP

## Chạy backend

Tạo môi trường Python, cài các gói trong `requirements.txt`, rồi chạy:

```powershell
$env:ADMIN_EMAIL = "admin@example.vn"
$env:ADMIN_PASSWORD = "<mật khẩu quản trị riêng, tối thiểu 12 ký tự>"
uvicorn reloop_backend:app --port 8000
```

`ADMIN_EMAIL` và `ADMIN_PASSWORD` đều cần được cấu hình để bật tài khoản quản trị. Không có thông tin mặc định trong mã nguồn; nếu cấu hình thiếu hoặc mật khẩu ngắn hơn 12 ký tự, backend không tạo tài khoản admin. Khi email đã tồn tại trong cơ sở dữ liệu, backend chỉ đổi mật khẩu nếu mật khẩu hiện tại không khớp cấu hình và thu hồi các phiên cũ.

Có thể đặt `RELOOP_DB_PATH` để chọn vị trí cơ sở dữ liệu SQLite. Các tính năng AI cần `VILAO_API_KEY` hoặc `KIMI_API_KEY`.
