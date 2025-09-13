from kvstore import KVStore
from helpers import now_ms

DATA_DIR = "data_test"

def handle_command(cmd: str):
    parts = cmd.strip().split()
    if not parts:
        return None

    action = parts[0].lower()
    store = KVStore(DATA_DIR)

    try:
        match action:
            case "-set":
                # -set <key> <value> Optional[-t <expiry_ms>]
                if len(parts) < 3:
                    print("Usage: -set <key> <value> [-t <expiry_ms>]")
                    return None

                key, value = parts[1], parts[2]

                # TTL flag: -t <expiry_ms>
                if len(parts) >= 5:
                    ttl_flag, expr = parts[3], parts[4]
                    if ttl_flag == "-t":
                        try:
                            secs = int(expr)
                        except ValueError:
                            print("time must be an integer, Usage: -t <secs>")
                            return None
                        expiry_ms = now_ms() + secs * 1000

                        store.put(key, value.encode("utf-8"), expiry_ms)
                        return True

                # TTL
                store.put(key, value.encode("utf-8"))
                print("Successfully set key:", key)
                return True
            case "-update":
                if len(parts) < 3:
                    print("Usage: -update <key> <value>")
                    return None
                key, value = parts[1], parts[2]
                store.put(key, value.encode("utf-8"))
                print("Successfully update key:", key)
                return None
            case "-delete":
                if len(parts) < 2:
                    print("Usage: -delete <key>")
                    return None
                key = parts[1]
                store.delete(key)
                print("Successfully delete key:", key)
                return None
            case "-get":
                if len(parts) < 2:
                    print("Usage: -get <key>")
                    return None
                key = parts[1]
                val = store.get(key)
                if val is None:
                    print("Failed to get key:", key)
                    return None
                print("Value:", val.decode("utf-8"))
            case "-exit":
                print("Exiting program...")
                return False
            case _:
                print("Unknown command. Use -set, -update, -delete, or -exit.")
                return None
    finally:
        store.close()


def main():
    print("Interactive CLI running. Use: -set, -update, -delete, or -exit")
    running = True
    while running:
        cmd = input("> ")
        res = handle_command(cmd)
        if res is False:
            running = False


if __name__ == "__main__":
    main()