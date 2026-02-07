import grizzly
import os

# Create a sample CSV
with open("test_show.csv", "w") as f:
    f.write("id,name,val\n1,Alice,10.5\n2,Bob,20.0\n3,Charlie,15.5\n")

try:
    print("Testing grizzly DataFrame.show()...")
    df = grizzly.read_csv("test_show.csv")
    
    print("\nDisplaying DataFrame using show():")
    df.show()
    
    print("\nTesting filtered show():")
    df.filter_eq("name", "Alice").show()

finally:
    if os.path.exists("test_show.csv"):
        os.remove("test_show.csv")
