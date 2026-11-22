import socket
import sys
import threading

HOST = "127.0.0.1"
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
barrier = threading.Barrier(3)
results = [None] * 3


def client(index):
    message = f"Привет от клиента {index}!".encode("utf-8")
    try:
        with socket.create_connection((HOST, PORT), timeout=5) as sock:
            sock.settimeout(5)
            barrier.wait(timeout=5)
            sock.sendall(message)
            reply = b""
            while len(reply) < len(message):
                chunk = sock.recv(4096)
                if not chunk:
                    break
                reply += chunk
            if reply != message:
                raise RuntimeError(f"ожидалось {message!r}, получено {reply!r}")
            results[index - 1] = f"Клиент {index}: отправлено '{message.decode()}', получено '{reply.decode()}' — OK"
    except Exception as exc:
        results[index - 1] = f"Клиент {index}: ОШИБКА: {exc}"


threads = [threading.Thread(target=client, args=(i,)) for i in range(1, 4)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
for result in results:
    print(result)
if any(not result.endswith("— OK") for result in results):
    sys.exit(1)
print("Результат: все три одновременных клиента получили точное эхо.")
