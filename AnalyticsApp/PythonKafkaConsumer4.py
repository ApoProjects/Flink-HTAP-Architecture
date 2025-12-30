import os
import time
import json
from confluent_kafka import Consumer
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

BOOTSTRAP = os.getenv("BOOTSTRAP", "localhost:29092")

TOPIC_TOTAL = "agg-total-cart"
TOPIC_USER  = "agg-user-cart"
TOPIC_PROD  = "agg-product-sales"

TOP_N_USERS = int(os.getenv("TOP_N_USERS", "10"))
TOP_N_PRODS = int(os.getenv("TOP_N_PRODS", "10"))

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "flink-agg-dashboard",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([TOPIC_TOTAL, TOPIC_USER, TOPIC_PROD])

# In-memory "latest state" (this is your table-in-app)
total_state = {"total_price": 0.0, "total_items": 0}
user_state = {}     # userId -> {"userName":..., "items":..., "total_price":..., "distinct_products":...}
prod_state = {}     # productId -> {"name":..., "sold_count":..., "revenue":...}

def drain_kafka(max_messages=500, max_seconds=0.2):
    """Poll a bit and update the state maps."""
    start = time.time()
    n = 0
    while n < max_messages and (time.time() - start) < max_seconds:
        msg = consumer.poll(0.0)
        if msg is None:
            break
        if msg.error():
            # ignore transient errors in UI loop
            break

        topic = msg.topic()
        key_b = msg.key()
        val_b = msg.value()

        try:
            key = json.loads(key_b.decode("utf-8")) if key_b else None
        except Exception:
            continue

        # upsert-kafka: value can be NULL (tombstone) => delete
        if val_b is None:
            val = None
        else:
            try:
                val = json.loads(val_b.decode("utf-8"))
            except Exception:
                continue

        if topic == TOPIC_TOTAL:
            # key looks like {"metric":"all"} if you used metric as PK; or {"metric":"all"} / {"metric":"all"}-like
            if val is None:
                total_state["total_price"] = 0.0
                total_state["total_items"] = 0
            else:
                total_state["total_price"] = float(val.get("total_price", 0.0) or 0.0)
                total_state["total_items"] = int(val.get("total_items", 0) or 0)

        elif topic == TOPIC_USER:
            if not key:
                continue
            # key is {"userId":123}
            uid = key.get("userId")
            if uid is None:
                continue
            if val is None:
                user_state.pop(uid, None)
            else:
                user_state[uid] = {
                    "userName": val.get("userName", ""),
                    "items": int(val.get("items", 0) or 0),
                    "total_price": float(val.get("total_price", 0.0) or 0.0),
                    "distinct_products": int(val.get("distinct_products", 0) or 0),
                }

        elif topic == TOPIC_PROD:
            if not key:
                continue
            # key is {"productId":10}
            pid = key.get("productId")
            if pid is None:
                continue
            if val is None:
                prod_state.pop(pid, None)
            else:
                prod_state[pid] = {
                    "name": val.get("name", ""),
                    "sold_count": int(val.get("sold_count", 0) or 0),
                    "revenue": float(val.get("revenue", 0.0) or 0.0),
                }

        n += 1

# --- Matplotlib setup (3 plots) ---
plt.ion()
fig, axes = plt.subplots(3, 1, figsize=(10, 10))
ax_total, ax_users, ax_prods = axes

def render(_frame):
    drain_kafka()

    # 1) Total revenue/items (single snapshot bars)
    ax_total.clear()
    ax_total.set_title("Total cart revenue (all time) + total items")
    labels = ["total_price", "total_items"]
    values = [total_state["total_price"], total_state["total_items"]]
    ax_total.bar(labels, values)
    ax_total.set_ylabel("value")

    # 2) Per-user totals (top N by total_price)
    ax_users.clear()
    ax_users.set_title(f"Top {TOP_N_USERS} users by total_price")
    items = []
    for uid, v in user_state.items():
        name = v.get("userName") or str(uid)
        items.append((name, v.get("total_price", 0.0)))
    items.sort(key=lambda x: x[1], reverse=True)
    items = items[:TOP_N_USERS]

    if items:
        names = [x[0] for x in items]
        totals = [x[1] for x in items]
        ax_users.bar(names, totals)
        ax_users.tick_params(axis="x", rotation=30)
        ax_users.set_ylabel("total_price")
    else:
        ax_users.text(0.5, 0.5, "No user data yet", ha="center", va="center")
        ax_users.set_axis_off()

    # 3) Best sold products (top N by sold_count)
    ax_prods.clear()
    ax_prods.set_title(f"Top {TOP_N_PRODS} products by sold_count")
    pitems = []
    for pid, v in prod_state.items():
        name = v.get("name") or str(pid)
        pitems.append((name, v.get("sold_count", 0)))
    pitems.sort(key=lambda x: x[1], reverse=True)
    pitems = pitems[:TOP_N_PRODS]

    if pitems:
        pnames = [x[0] for x in pitems]
        counts = [x[1] for x in pitems]
        ax_prods.bar(pnames, counts)
        ax_prods.tick_params(axis="x", rotation=30)
        ax_prods.set_ylabel("sold_count")
    else:
        ax_prods.text(0.5, 0.5, "No product data yet", ha="center", va="center")
        ax_prods.set_axis_off()

    fig.tight_layout()

ani = FuncAnimation(fig, render, interval=1000)  # refresh every 1s

print(f"Dashboard running. BOOTSTRAP={BOOTSTRAP}")
print("Close the window or Ctrl+C to stop.")

try:
    plt.show(block=True)
finally:
    consumer.close()