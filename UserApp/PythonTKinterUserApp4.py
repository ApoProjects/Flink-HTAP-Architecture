import os
import json
import threading
import queue
import tkinter as tk
from tkinter import ttk
from confluent_kafka import Producer, Consumer
import sqlite3


DB_PATH = r"C:\Users\apost\OneDrive\Desktop\BAneu\Implementation4\products.db" #os.getenv("PRODUCTS_DB", "products.db")


def load_products(db_path: str):
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("PRAGMA database_list;")
    print("SQLite database_list:", cur.fetchall())
    db_path = os.path.abspath(db_path)
    print("Opening DB:", db_path, "exists:", os.path.exists(db_path))
    try:
        cur = con.cursor()
        cur.execute("SELECT id, name, price FROM products ORDER BY id")
        rows = cur.fetchall()
        return [{"productId": int(i), "name": n, "price": float(p)} for (i, n, p) in rows]
    finally:
        con.close()
        
        
def load_users(db_path: str):
    db_path = os.path.abspath(db_path)
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT id, name FROM users ORDER BY id")
        rows = cur.fetchall()
        # returns list of {"userId":..., "userName":...}
        return [{"userId": int(i), "userName": str(u)} for (i, u) in rows]
    finally:
        con.close()


BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:29092")

TOPIC_EVENTS = "cart-events"
TOPIC_USER_CART = "agg-user-cart"
TOPIC_USER_ITEMS = "agg-user-items"
TOPIC_USERS = "users"  # upsert-kafka output topic from Flink
TOPIC_USER_EVENTS = "user-events"


# Simple product catalog (replace later if you have a product topic)


PRODUCTS = load_products(DB_PATH)
user_catalog = {}

#PRODUCTS = [
#    {"productId": 1, "name": "Keyboard", "price": 49.99},
#   {"productId": 2, "name": "Mouse", "price": 19.99},
 #   {"productId": 3, "name": "Monitor", "price": 199.99},
#    {"productId": 4, "name": "USB-C Cable", "price": 9.99},
#]

# ---- Kafka producer ----
producer = Producer({"bootstrap.servers": BOOTSTRAP})

def send_cart_event(user_id: int, user_name: str, product: dict, delta: int, action: str):
    evt = {
        "userId": int(user_id),
        "userName": user_name,
        "productId": int(product["productId"]),
        "name": product["name"],
        "unitPrice": float(product["price"]),
        "delta": int(delta),
        "action": action
    }
    producer.produce(TOPIC_EVENTS, value=json.dumps(evt).encode("utf-8"))
    producer.poll(0)

def send_user_event(user_id: int, user_name: str | None, event_type: str):
    evt = {
        "userId": int(user_id),
        "eventType": event_type
    }
    if user_name is not None and user_name.strip() != "":
        evt["userName"] = user_name.strip()

    producer.produce(TOPIC_USER_EVENTS, value=json.dumps(evt).encode("utf-8"))
    producer.poll(0)

# ---- Shared state (updated from Kafka thread) ----
# userId -> summary
users = {}  # userId -> {"userName":..., "items":..., "total_price":..., "distinct_products":...}
# (userId, productId) -> line item
items = {}  # (uid,pid) -> {"name":..., "qty":..., "subtotal":...}

# UI update queue (thread-safe)
uiq: "queue.Queue[dict]" = queue.Queue()

def kafka_thread():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "tk-cart-ui-" + os.urandom(6).hex(),
        "auto.offset.reset": "earliest",
    })
    c.subscribe([TOPIC_USERS, TOPIC_USER_CART, TOPIC_USER_ITEMS])

    try:
        while True:
            msg = c.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                continue

            topic = msg.topic()
            key_b = msg.key()
            val_b = msg.value()

            # upsert-kafka keys/values are JSON; value may be None (tombstone delete)
            try:
                key = json.loads(key_b.decode("utf-8")) if key_b else None
            except Exception:
                continue

            if val_b is None:
                val = None
            else:
                try:
                    val = json.loads(val_b.decode("utf-8"))
                except Exception:
                    continue

            changed_user_id = None

            if topic == TOPIC_USER_CART and key:
                uid = key.get("userId")
                if uid is None:
                    continue
                changed_user_id = uid

                if val is None:
                    users.pop(uid, None)
                else:
                    users[uid] = {
                        "userName": val.get("userName", str(uid)),
                        "items": int(val.get("items", 0) or 0),
                        "total_price": float(val.get("total_price", 0.0) or 0.0),
                        "distinct_products": int(val.get("distinct_products", 0) or 0),
                    }
                    
            if topic == TOPIC_USERS and key:
                uid = key.get("userId")
                if uid is None:
                    continue

                    # upsert-kafka: val == None means DELETE (tombstone)
                if val is None:
                    user_catalog.pop(uid, None)
                else:
                        # value is like {"userId":1,"userName":"Alice"} (or may omit userId)
                    uname = val.get("userName", "")
                    if uname:
                        user_catalog[uid] = uname

                uiq.put({"type": "users_update"})
                continue
            

            elif topic == TOPIC_USER_ITEMS and key:
                uid = key.get("userId")
                pid = key.get("productId")
                if uid is None or pid is None:
                    continue
                changed_user_id = uid

                k = (uid, pid)
                if val is None:
                    items.pop(k, None)
                else:
                    items[k] = {
                        "name": val.get("name", str(pid)),
                        "qty": int(val.get("qty", 0) or 0),
                        "subtotal": float(val.get("subtotal", 0.0) or 0.0),
                    }

            if changed_user_id is not None:
                uiq.put({"type": "update", "userId": changed_user_id})

    finally:
        c.close()

# ---- Tkinter UI ----
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Cart UI (Kafka → Flink → Kafka)")

        self.selected_user_id = tk.StringVar(value="1")
        self.selected_user_name = tk.StringVar(value="User1")

        top = ttk.Frame(self, padding=8)
        top.pack(fill="x")

        ttk.Label(top, text=f"BOOTSTRAP: {BOOTSTRAP}").pack(side="left")

        ttk.Label(top, text="   User:").pack(side="left")
        self.user_combo = ttk.Combobox(top, width=30, state="readonly")
        self.user_combo.pack(side="left", padx=6)
        self.user_combo.bind("<<ComboboxSelected>>", self.on_user_selected)
        
        # Load users from DB once
       # self.db_users = load_users(USER_DB_PATH)

        # Fill dropdown from DB
       # self.user_combo["values"] = [f'{u["userId"]} — {u["userName"]}' for u in self.db_users]
        #if self.db_users:
        #    self.user_combo.current(0)
         #   self.selected_user_id.set(str(self.db_users[0]["userId"]))
         #   self.selected_user_name.set(self.db_users[0]["userName"])
        

        ttk.Label(top, text="UserId:").pack(side="left")
        self.user_id_entry = ttk.Entry(top, width=6, textvariable=self.selected_user_id)
        self.user_id_entry.pack(side="left", padx=4)

        ttk.Label(top, text="UserName:").pack(side="left")
        self.user_name_entry = ttk.Entry(top, width=12, textvariable=self.selected_user_name)
        self.user_name_entry.pack(side="left", padx=4)

        ttk.Button(top, text="Set/Use User", command=self.apply_manual_user).pack(side="left", padx=6)
        ttk.Button(top, text="Register", command=self.register_user).pack(side="left", padx=6)
        ttk.Button(top, text="Delete", command=self.delete_user).pack(side="left", padx=6)

        mid = ttk.Frame(self, padding=8)
        mid.pack(fill="both", expand=True)

        left = ttk.Frame(mid)
        left.pack(side="left", fill="y", padx=(0, 10))

        ttk.Label(left, text="Products").pack(anchor="w")

        for p in PRODUCTS:
            row = ttk.Frame(left)
            row.pack(fill="x", pady=3)

            ttk.Label(row, text=f'{p["name"]} (€{p["price"]})').pack(side="left")

            ttk.Button(row, text="+", width=3,
                    command=lambda prod=p: self.add_delta(prod, +1)).pack(side="right", padx=(4, 0))

            ttk.Button(row, text="-", width=3,
                    command=lambda prod=p: self.add_delta(prod, -1)).pack(side="right", padx=(4, 0))

            ttk.Button(row, text="del", width=4,
                    command=lambda prod=p: self.delete_product(prod)).pack(side="right", padx=(4, 0))

        right = ttk.Frame(mid)
        right.pack(side="left", fill="both", expand=True)
        
        row = ttk.Frame(left)
        row.pack(fill="x", pady=3)

        
        

        self.summary_lbl = ttk.Label(right, text="Items: 0    Total: 0.00")
        self.summary_lbl.pack(anchor="w", pady=(0, 8))

        self.tree = ttk.Treeview(right, columns=("product", "qty", "subtotal"), show="headings", height=14)
        self.tree.heading("product", text="Product")
        self.tree.heading("qty", text="Qty")
        self.tree.heading("subtotal", text="Subtotal")
        self.tree.column("product", width=220)
        self.tree.column("qty", width=60, anchor="center")
        self.tree.column("subtotal", width=100, anchor="e")
        self.tree.pack(fill="both", expand=True)

        self.status = ttk.Label(self, text="Waiting for Flink updates…")
        self.status.pack(fill="x", padx=8, pady=(0, 8))

        # start UI polling of queue
        self.after(200, self.process_queue)
        self.refresh_user_list()
        self.refresh_cart_view()

    def refresh_user_list(self):
        
        entries = [f"{uid} — {uname}" for uid, uname in sorted(user_catalog.items(), key=lambda x: x[0])]

        if not entries:
            entries = [f'{self.selected_user_id.get()} — {self.selected_user_name.get()}']

        current = self.user_combo.get()
        self.user_combo["values"] = entries

        # keep current selection if it still exists; otherwise pick first
        if current in entries:
            self.user_combo.set(current)
        else:
            self.user_combo.current(0)
            # also update fields to match selection
            self.on_user_selected()

    def on_user_selected(self, _evt=None):
           
        s = self.user_combo.get()
        try:
            uid_str, uname = s.split("—", 1)
            self.selected_user_id.set(uid_str.strip())
            self.selected_user_name.set(uname.strip())
        except Exception:
            pass
        self.refresh_cart_view()

    def apply_manual_user(self):
        self.refresh_cart_view()

    def add_product(self, prod):
        try:
            uid = int(self.selected_user_id.get())
        except Exception:
            self.status.config(text="Invalid userId")
            return
        uname = self.selected_user_name.get().strip() or f"User{uid}"
        self.selected_user_name.set(uname)

        send_cart_event(uid, uname, prod)
        self.status.config(text=f"Sent event: +{prod['name']} for user {uid} (waiting for Flink…)")

    def refresh_cart_view(self):
        # Clear table
        for row in self.tree.get_children():
            self.tree.delete(row)

        try:
            uid = int(self.selected_user_id.get())
        except Exception:
            uid = 1

        # Fill items for selected user
        rows = []
        for (u, _pid), v in items.items():
            if u == uid:
                rows.append((v["name"], v["qty"], v["subtotal"]))
        rows.sort(key=lambda x: x[0])

        subtotal_sum = 0.0
        for name, qty, sub in rows:
            subtotal_sum += float(sub)
            self.tree.insert("", "end", values=(name, qty, f"{sub:.2f}"))

        # Prefer summary from agg-user-cart if present
        s = users.get(uid)
        if s:
            self.summary_lbl.config(text=f'Items: {s["items"]}    Total: {s["total_price"]:.2f}    Distinct: {s["distinct_products"]}')
        else:
            self.summary_lbl.config(text=f"Items: ?    Total: {subtotal_sum:.2f}")

        self.refresh_user_list()


    def register_user(self):
        try:
            uid = int(self.selected_user_id.get())
        except Exception:
            self.status.config(text="Invalid userId")
            return

        uname = self.selected_user_name.get().strip()
        if not uname:
            self.status.config(text="UserName required for REGISTER")
            return

        send_user_event(uid, uname, "REGISTER")
        self.status.config(text=f"Sent REGISTER for {uid} — {uname} (wait for Flink/users topic)")



    def delete_user(self):
        try:
            uid = int(self.selected_user_id.get())
        except Exception:
            self.status.config(text="Invalid userId")
            return

        # userName not required for DELETE
        send_user_event(uid, None, "DELETE")
        self.status.config(text=f"Sent DELETE for userId {uid} (wait for Flink/users topic)")



    def process_queue(self):
        changed = False
        while True:
            try:
                evt = uiq.get_nowait()
            except queue.Empty:
                break
            if evt.get("type") in ("update", "users_update"):
                # Only redraw if the update is for the selected user OR if it's a new user list
                changed = True

        if changed:
            self.refresh_cart_view()
            self.status.config(text="Updated from Flink (Kafka aggregates)")

        self.after(200, self.process_queue)
        
        
    def add_delta(self, prod, delta):
        uid = int(self.selected_user_id.get())
        uname = self.selected_user_name.get().strip() or f"User{uid}"
        self.selected_user_name.set(uname)

        action = "ADD" if delta > 0 else "REMOVE"

        # Optional safety: don't remove below 0 (use your current cart state)
        cur_qty = 0
        for (u, pid), v in items.items():
            if u == uid and pid == prod["productId"]:
                cur_qty = v["qty"]
                break
        if delta < 0 and cur_qty <= 0:
            self.status.config(text="Qty already 0")
            return

        send_cart_event(uid, uname, prod, delta=delta, action=action)
        self.status.config(text=f"Sent {action} {prod['name']} ({delta})")

    def delete_product(self, prod):
        uid = int(self.selected_user_id.get())
        uname = self.selected_user_name.get().strip() or f"User{uid}"
        self.selected_user_name.set(uname)

        cur_qty = 0
        for (u, pid), v in items.items():
            if u == uid and pid == prod["productId"]:
                cur_qty = v["qty"]
                break
        if cur_qty <= 0:
            self.status.config(text="Nothing to delete")
            return

        send_cart_event(uid, uname, prod, delta=-cur_qty, action="DELETE")
        self.status.config(text=f"Deleted {prod['name']} (qty {cur_qty})")

if __name__ == "__main__":
    t = threading.Thread(target=kafka_thread, daemon=True)
    t.start()
    app = App()
    app.mainloop()
    
    
    
    
    
    
    
    
    