#!/usr/bin/env python3
"""Xóa tất cả transcriptions và files trên Soniox để giải phóng quota."""

import os
import sys
import concurrent.futures as cf

SONIOX_API_KEY = os.environ.get("SONIOX_API_KEY", "")

if not SONIOX_API_KEY:
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                if line.startswith("SONIOX_API_KEY="):
                    SONIOX_API_KEY = line.split("=", 1)[1].strip()
                    break

if not SONIOX_API_KEY:
    print("Set SONIOX_API_KEY env var hoặc paste key vào script")
    sys.exit(1)

from soniox.client import SonioxClient

client = SonioxClient(api_key=SONIOX_API_KEY)

WORKERS = int(os.environ.get("SONIOX_WORKERS", "16"))
PAGE = 200  # max page size to minimize round trips


def collect_all_ids(label, list_page):
    """Yields all ids from a paginated list endpoint."""
    cursor = None
    page_count = 0
    while True:
        try:
            resp = list_page(limit=PAGE, cursor=cursor)
        except Exception as e:
            print(f"  [{label}] lỗi list (cursor={cursor!r}): {e}")
            break
        items = getattr(resp, "files", None) or getattr(resp, "transcriptions", None) or []
        if not items and cursor is None:
            return
        for it in items:
            yield getattr(it, "id", None), getattr(it, "file_id", None)
        page_count += 1
        cursor = getattr(resp, "next_page_cursor", None)
        if not cursor:
            break
    print(f"  [{label}] đã quét {page_count} trang")


def delete_one(kind, file_id):
    if not file_id:
        return None
    try:
        if kind == "file":
            client.files.delete_if_exists(file_id)
        else:
            client.stt.delete_if_exists(file_id)
        return file_id
    except Exception as e:
        return f"ERR[{file_id}]: {e}"


def parallel_delete(kind, ids):
    deleted = 0
    errors = 0
    error_samples = []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(delete_one, kind, _id) for _id in ids if _id]
        for i, fut in enumerate(cf.as_completed(futures), 1):
            r = fut.result()
            if r is None:
                continue
            if isinstance(r, str) and r.startswith("ERR"):
                errors += 1
                if len(error_samples) < 5:
                    error_samples.append(r)
            else:
                deleted += 1
            if i % 100 == 0:
                print(f"    [{kind}] tiến độ {i}/{len(futures)} (deleted={deleted}, errors={errors})")
    return deleted, errors, error_samples


def run():
    # 1) Thu gom id files + transcriptions
    print("Đang lấy danh sách files...")
    file_ids = list({fid for fid, _ in collect_all_ids("files", client.files.list) if fid})
    print(f"  Tìm thấy {len(file_ids)} files.")

    print("Đang lấy danh sách transcriptions...")
    pairs = list(collect_all_ids("transcriptions", client.stt.list))
    trans_ids = [tid for tid, _ in pairs if tid]
    # Bổ sung thêm file_id gắn với transcription mà có thể không nằm trong list files
    extra_file_ids = {fid for _, fid in pairs if fid}
    print(f"  Tìm thấy {len(trans_ids)} transcriptions, {len(extra_file_ids)} file_id gắn với chúng.")

    if not file_ids and not trans_ids:
        print("Không có gì để xóa.")
        return

    # 2) Xóa transcriptions trước (xóa transcription thường KHÔNG xóa file đi kèm -> file còn chiếm quota)
    #    Nên xóa song song cả 2 luôn để nhanh.
    print(f"\nBắt đầu xóa song song với {WORKERS} workers...")
    f_del, f_err, f_samples = parallel_delete("file", file_ids)
    print(f"  Files: deleted={f_del}, errors={f_err}")
    if f_samples:
        for s in f_samples:
            print(f"    {s}")

    t_del, t_err, t_samples = parallel_delete("transcription", trans_ids)
    print(f"  Transcriptions: deleted={t_del}, errors={t_err}")
    if t_samples:
        for s in t_samples:
            print(f"    {s}")

    # 3) Sweep lần 2: xóa nốt các file còn sót (gắn với transcription, hoặc sinh ra trong quá trình xóa)
    print("\nSweep lần 2 cho file còn sót...")
    remaining_files = list({fid for fid, _ in collect_all_ids("files", client.files.list) if fid})
    print(f"  Còn {len(remaining_files)} files.")
    if remaining_files:
        f2_del, f2_err, f2_samples = parallel_delete("file", remaining_files)
        print(f"  Files (sweep 2): deleted={f2_del}, errors={f2_err}")
        for s in f2_samples:
            print(f"    {s}")

    remaining_trans = [tid for tid, _ in collect_all_ids("transcriptions", client.stt.list) if tid]
    print(f"  Còn {len(remaining_trans)} transcriptions.")
    if remaining_trans:
        t2_del, t2_err, t2_samples = parallel_delete("transcription", remaining_trans)
        print(f"  Transcriptions (sweep 2): deleted={t2_del}, errors={t2_err}")
        for s in t2_samples:
            print(f"    {s}")

    print("\nXong.")


if __name__ == "__main__":
    run()
