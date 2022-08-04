from pathlib import Path

from json2tab import open_file


def test_flatten_output():
    wd = Path(r'C:\Users\nguyenso\PycharmProjects\json-parsing')
    csv_file1 = wd / 'output/test_full_site.csv'  # correct output
    csv_file2 = wd / 'src/output-dev/test_full_site.csv'  # test output

    f1 = open_file(str(csv_file1), mode="rb")
    f2 = open_file(str(csv_file2), mode="rb")

    for line1, line2 in zip(f1, f2):
        print(line1)
        print(line2)
        assert (line1 == line2)
        break

    for line1, line2 in zip(f1, f2):
        assert (line1 == line2)

    f1.close()
    f2.close()


test_flatten_output()
