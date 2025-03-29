import json
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
import os

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            yield message
        except Exception as e:
            print("Parse error:", e)


class FilterPurchases(beam.DoFn):
    def process(self, element):
        if element.get("event_type") == "purchase":
            yield element


class FormatForKey(beam.DoFn):
    def process(self, element):
        yield ((element["region"], element["device"]), 1)


class FormatForBigQuery(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        (region, device), count = element
        yield {
            'region': region,
            'device': device,
            'event_count': count,
            'window_start': window.start.to_utc_datetime().isoformat(),
            'window_end': window.end.to_utc_datetime().isoformat()
        }


def load_config(path="pipeline_config.json"):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}



def run(argv=None):
    parser = argparse.ArgumentParser()
    config = load_config()

    parser.add_argument('--input_topic', default=config.get("input_topic"))
    parser.add_argument('--output_table', default=config.get("output_table"))
    parser.add_argument('--alert_topic', default=config.get("alert_topic"))
    parser.add_argument('--aggregation_window_sec', type=int, default=10, help='Fixed window duration in seconds for BQ aggregation')
    parser.add_argument('--alert_window_sec', type=int, default=1, help='Fixed window duration in seconds for alerting')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        purchases = (
            p
            | "Read PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "Parse JSON" >> beam.ParDo(ParseMessage())
            | "Filter 'purchase'" >> beam.ParDo(FilterPurchases())
        )

        # Aggregation to BigQuery using dynamic window duration
        (
            purchases
            | f"Window {known_args.aggregation_window_sec}s" >> beam.WindowInto(FixedWindows(known_args.aggregation_window_sec))
            | "Key by region/device (agg)" >> beam.ParDo(FormatForKey())
            | "Count agg" >> beam.CombinePerKey(sum)
            | "Format for BQ" >> beam.ParDo(FormatForBigQuery())
            | "Write to BQ" >> WriteToBigQuery(
                known_args.output_table,
                schema='region:STRING, device:STRING, event_count:INTEGER, window_start:TIMESTAMP, window_end:TIMESTAMP',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # Alerting to Pub/Sub using dynamic alert window
        (
            purchases
            | f"Window {known_args.alert_window_sec}s" >> beam.WindowInto(FixedWindows(known_args.alert_window_sec))
            | "Key by region/device (alert)" >> beam.ParDo(FormatForKey())
            | "Count alert" >> beam.CombinePerKey(sum)
            | "Filter alerts" >> beam.Filter(lambda x: x[1] >= 100)
            | "Format alert JSON" >> beam.Map(lambda x: {
                "region": x[0][0],
                "device": x[0][1],
                "event_count": x[1]
            })
            | "Encode to JSON" >> beam.Map(lambda x: json.dumps(x).encode('utf-8'))
            | "Write alert to PubSub" >> beam.io.WriteToPubSub(topic=known_args.alert_topic)
        )

