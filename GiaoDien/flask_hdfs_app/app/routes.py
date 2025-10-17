from flask import Blueprint, render_template, request, redirect, url_for, flash
from .spark_utils import list_files, read_file, write_file, delete_file

main = Blueprint("main", __name__)

@main.route("/")
def dashboard():
    files = list_files()
    return render_template("dashboard.html", files=files)

@main.route("/hdfs", methods=["GET", "POST"])
def hdfs_crud():
    if request.method == "POST":
        action = request.form.get("action")
        path = request.form.get("path")
        content = request.form.get("content", "")

        if not path:
            flash("❗ Bạn phải nhập đường dẫn file.", "error")
            return redirect(url_for("main.hdfs_crud"))

        if action == "read":
            data = read_file(path)
            flash(f"Nội dung file {path}:\n{data}", "info")

        elif action == "write":
            write_file(path, content)
            flash(f"✅ Đã ghi file {path}", "success")

        elif action == "delete":
            delete_file(path)
            flash(f"🗑️ Đã xóa file {path}", "warning")

        return redirect(url_for("main.hdfs_crud"))

    return render_template("hdfs_crud.html")
