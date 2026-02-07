import grizzly
import pytest
import os
import shutil

@pytest.fixture
def sample_csv():
    path = "test_features.csv"
    with open(path, "w") as f:
        f.write("id,value,name\n1,10.5,Alice\n2,20.0,Bob\n3,100.1,Charlie\n4,50.5,David\n")
    yield path
    if os.path.exists(path):
        os.remove(path)

def test_shape(sample_csv):
    df = grizzly.read_csv(sample_csv)
    assert df.shape == (4, 3)
    print(f"Shape verified: {df.shape}")

def test_query_numeric(sample_csv):
    df = grizzly.read_csv(sample_csv)
    
    # Test <
    df_q = df.query("value < 30")
    assert df_q.shape == (2, 3)
    
    # Test >
    df_q = df.query("value > 30")
    assert df_q.shape == (2, 3)
    
    # Test ==
    df_q = df.query("id == 3")
    assert df_q.shape == (1, 3)
    
    print("Numeric queries verified")

def test_query_string(sample_csv):
    df = grizzly.read_csv(sample_csv)
    
    # Test ==
    df_q = df.query("name == Alice")
    assert df_q.shape == (1, 3)
    
    # Test !=
    df_q = df.query("name != Alice")
    assert df_q.shape == (3, 3)
    
    print("String queries verified")
