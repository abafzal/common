import pandas as pd
import argparse


def excel_to_csv(infile, outfile, input_encoding='latin-1', output_encoding='utf-8', input_delimiter=',', output_delimiter=','):
    data = pd.read_excel(infile, encoding=input_encoding, delimiter=input_delimiter, dtype=object)
    data.to_csv(outfile, encoding=output_encoding, sep=output_delimiter, index=None)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, required=True)
    parser.add_argument('--outfile', type=str, required=True)
    parser.add_argument('--input_encoding', type=str, required=False, default='latin-1')
    parser.add_argument('--output_encoding', type=str, required=False, default='utf-8')
    parser.add_argument('--input_delimiter', type=str, required=False, default=',')
    parser.add_argument('--output_delimiter', type=str, required=False, default=',')

    args = parser.parse_args()
    excel_to_csv(infile=args.infile,
                 outfile=args.outfile,
                 input_encoding=args.input_encoding,
                 output_encoding=args.output_encoding,
                 input_delimiter=args.input_delimiter,
                 output_delimiter=args.output_delimiter)
