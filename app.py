from flask import Flask, render_template, request, Response, send_from_directory
import threading, time, json, queue, os, sys, logging

# Ensure project root is on path so we can import Forecaster
sys.path.append(os.path.dirname(__file__))
import Forecaster  # noqa: E402

app = Flask(__name__, template_folder="templates")


def runner(status_q: "queue.Queue[str]") -> None:
    """Run forecasting pipeline and put status strings into queue."""
    def log(msg: str) -> None:
        status_q.put(json.dumps({"stage": msg, "ts": time.time()}))

    start = time.perf_counter()
    log("started")

    # Monkey-patch Forecaster logging to queue
    Forecaster.logger.setLevel("INFO")
    handler = logging.Handler()
    handler.emit = lambda record: log(record.getMessage())
    Forecaster.logger.addHandler(handler)

    import builtins
    original_input = builtins.input
    builtins.input = lambda _: ""  # auto-enter for prompt
    try:
        Forecaster.main()
    finally:
        builtins.input = original_input
        elapsed = time.perf_counter() - start
        log(f"done:{elapsed:.2f}")
        status_q.put("__END__")


@app.route("/")
def index():
    return render_template("index.html", workbook=Forecaster.SOURCE_FILE)


@app.route("/run", methods=["GET", "POST"])
def run_forecast():
    status_q: "queue.Queue[str]" = queue.Queue()
    threading.Thread(target=runner, args=(status_q,), daemon=True).start()

    def event_stream():
        while True:
            msg = status_q.get()
            if msg == "__END__":
                break
            yield f"data: {msg}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    app.run(debug=True)
