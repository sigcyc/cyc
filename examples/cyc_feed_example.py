import cyc_feed
from cyc import *

reader = cyc_feed.Reader("MARKET_DATA", version=2)     # subscribe at the tail

batch = reader.poll_batch(4096)
df = Df(pl.DataFrame(batch.messages))

for key in df.columns:
    print(key, df[key].dtype)
