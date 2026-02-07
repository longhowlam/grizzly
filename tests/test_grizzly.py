import grizzly
import pytest
import os
import shutil

@pytest.fixture(scope="module")
def data_dir():
    dp = "tmp_test_data"
    os.makedirs(dp, exist_ok=True)
    yield dp
    shutil.rmtree(dp)

@pytest.fixture
def sample_csv(data_dir):
    path = os.path.join(data_dir, "test.csv")
    with open(path, "w") as f:
        f.write("name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n")
    return path

def test_csv_io(sample_csv, data_dir):
    df = grizzly.read_csv(sample_csv)
    assert df.row_count() == 3
    out_path = os.path.join(data_dir, "out.csv")
    df.to_csv(out_path)
    df2 = grizzly.read_csv(out_path)
    assert df2.row_count() == 3

def test_parquet_io(sample_csv, data_dir):
    df = grizzly.read_csv(sample_csv)
    path = os.path.join(data_dir, "out.parquet")
    df.to_parquet(path)
    df2 = grizzly.read_parquet(path)
    assert df2.row_count() == 3

def test_json_io(sample_csv, data_dir):
    df = grizzly.read_csv(sample_csv)
    path = os.path.join(data_dir, "out.json")
    df.to_json(path)
    df2 = grizzly.read_json(path)
    assert df2.row_count() == 3

def test_excel_io(sample_csv, data_dir):
    df = grizzly.read_csv(sample_csv)
    path = os.path.join(data_dir, "out.xlsx")
    df.to_excel(path)
    df2 = grizzly.read_excel(path)
    assert df2.row_count() == 3

def test_filtering(sample_csv):
    df = grizzly.read_csv(sample_csv)
    df_f = df.filter_eq("name", "Alice")
    assert df_f.row_count() == 1

def test_sorting(sample_csv):
    df = grizzly.read_csv(sample_csv)
    df_s = df.sort("age", ascending=True)
    assert df_s.row_count() == 3

def test_concat(sample_csv):
    df = grizzly.read_csv(sample_csv)
    df_c = df.concat(df)
    assert df_c.row_count() == 6

def test_groupby(sample_csv):
    df = grizzly.read_csv(sample_csv)
    df_g = df.groupby_sum("city", "age")
    assert df_g.row_count() == 3
    assert df_g.column_count() == 2

def test_join(sample_csv, data_dir):
    df1 = grizzly.read_csv(sample_csv)
    stats_path = os.path.join(data_dir, "stats.csv")
    with open(stats_path, "w") as f:
        f.write("name,salary\nAlice,100000\nBob,80000\nCharlie,120000\n")
    df2 = grizzly.read_csv(stats_path)
    df_j = df1.join(df2, on="name")
    assert df_j.row_count() == 3
    assert df_j.column_count() == 4
