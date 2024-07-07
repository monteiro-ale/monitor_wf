import subprocess
import sys
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileChangeHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory or event.src_path.endswith(".py"):
            restart_streamlit()

def restart_streamlit():
    print("Restarting Streamlit...")
    if hasattr(restart_streamlit, "process") and restart_streamlit.process:
        restart_streamlit.process.terminate()
    restart_streamlit.process = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "dashboard/app2.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

if __name__ == "__main__":
    observer = Observer()
    observer.schedule(FileChangeHandler(), ".", recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


#C:/Users/usuario/Desktop/monitor_wf