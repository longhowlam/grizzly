import grizzly
import pandas as pd
import os

# Create a sample CSV with different types and 15 rows
data = {
    "id": list(range(1, 16)),
    "name": [f"Person_{i}" for i in range(1, 16)],
    "score": [i * 1.5 for i in range(1, 16)],
    "active": [i % 2 == 0 for i in range(1, 16)]
}
df_pd = pd.DataFrame(data)
df_pd.to_csv("test_show_extended.csv", index=False)

try:
    print("Loading data into grizzly...")
    df = grizzly.read_csv("test_show_extended.csv")
    
    print("\nTesting default show() (should show 10 rows + 1 type row):")
    df.show()
    
    print("\nTesting show(n=5) (should show 5 rows + 1 type row):")
    df.show(n=5)
    
    print("\nTesting show(n=20) (should show all 15 rows + 1 type row):")
    df.show(n=20)

finally:
    if os.path.exists("test_show_extended.csv"):
        os.remove("test_show_extended.csv")
