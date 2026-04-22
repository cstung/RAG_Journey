# 🏢 Internal Chatbot

Chatbot RAG nội bộ: PDF → ChromaDB → OpenAI → Chat UI

## Cấu trúc

```
internal-chatbot/
├── backend/
│   ├── main.py          # FastAPI app
│   ├── rag.py           # RAG query logic
│   ├── ingest.py        # PDF ingestion
│   ├── static/
│   │   └── index.html   # Chat UI
│   ├── requirements.txt
│   └── Dockerfile
├── data/
│   ├── docs/            # ← Thả PDF vào đây
│   └── chroma/          # Vector DB (tự tạo)
├── docker-compose.yml
├── .env.example
└── README.md
```

## Khởi động

### 1. Cấu hình

```bash
cp .env.example .env
# Mở .env và điền OPENAI_API_KEY
```

### 2. Build & chạy

```bash
docker compose up -d --build
```

Truy cập: http://localhost:8300

### 3. Upload tài liệu

**Cách 1 - Qua giao diện web:**
- Vào http://localhost:8300
- Click "Chọn file" → chọn PDF → tự động index

**Cách 2 - Thả file và index hàng loạt (nếu có nhiều file):**
```bash
# Copy tất cả PDF vào thư mục docs
cp /path/to/your/pdfs/*.pdf ./data/docs/

# Trigger index hàng loạt
curl -X POST http://localhost:8300/api/ingest-all
```

**Cách 3 - Chạy trực tiếp trong container:**
```bash
docker exec internal-chatbot python ingest.py
```

## API

| Endpoint | Method | Mô tả |
|---|---|---|
| `/api/health` | GET | Kiểm tra trạng thái |
| `/api/stats` | GET | Thống kê số file/chunks |
| `/api/chat` | POST | Gửi câu hỏi |
| `/api/upload` | POST | Upload 1 file PDF |
| `/api/ingest-all` | POST | Index lại toàn bộ |

### Ví dụ gọi API:

```bash
curl -X POST http://localhost:8300/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "Quy trình xin nghỉ phép như thế nào?"}'
```

## Chi phí ước tính (OpenAI)

Với 100 file PDF (~500 trang):
- **Embedding khi ingest**: ~$0.05 (một lần duy nhất)
- **Mỗi câu hỏi**: ~$0.001-0.003 (gpt-4o-mini)
- 1000 câu hỏi/tháng ≈ **$1-3/tháng**

## Thêm Cloudflare Tunnel (optional)

```bash
# Nếu muốn truy cập từ ngoài mạng nội bộ
cloudflared tunnel --url http://localhost:8300
```

## Troubleshooting

**Lỗi "No extractable text":** PDF bị scan ảnh, cần OCR trước.
Dùng: `ocrmypdf input.pdf output.pdf` rồi upload lại.

**Kết quả không chính xác:** Thử tăng TOP_K trong .env (5 → 8).

**Xóa và index lại từ đầu:**
```bash
rm -rf ./data/chroma/*
curl -X POST http://localhost:8300/api/ingest-all
```
