import csv
import argparse


def txt_to_csv(infile, outfile, input_encoding='utf-8', output_encoding='utf-8', input_delimiter=',', output_delimiter=','):
    with open(infile, 'r', encoding=input_encoding) as inputfile:
        with open(outfile, 'w', encoding=output_encoding) as outputfile:
            csv_reader = csv.DictReader(inputfile, delimiter=input_delimiter)
            csv_writer = csv.DictWriter(outputfile, delimiter=output_delimiter, fieldnames=csv_reader.fieldnames)
            for row in csv_reader:
                csv_writer.writerow(row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--infile', type=str, required=True)
    parser.add_argument('--outfile', type=str, required=True)
    parser.add_argument('--input_encoding', type=str, required=False, default='utf-8')
    parser.add_argument('--output_encoding', type=str, required=False, default='utf-8')
    parser.add_argument('--input_delimiter', type=str, required=False, default=',')
    parser.add_argument('--output_delimiter', type=str, required=False, default=',')

    args = parser.parse_args()
    txt_to_csv(infile=args.infile,
               outfile=args.outfile,
               input_encoding=args.input_encoding,
               output_encoding=args.output_encoding,
               input_delimiter=args.input_delimiter,
               output_delimiter=args.output_delimiter)
