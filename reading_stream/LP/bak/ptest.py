import os
import sys
from os.path import dirname

import xmltodict

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))
from datetime import datetime

# from ami import constant, func, lp_config
from ami.constant import LOADPROFILE, QUALITYCODE


def parse_xml(xml: str) -> tuple:
    """
    Parse the xml file and return the dataframe

    Args:
        xml (str): xml

    Returns:
        tuple: dataframe, error dataframe, warning dataframe, file info
    """
    exitcode = 0

    # stored 讀表資料
    header_tmp = {}
    read_map = {}

    # create element tree object
    try:
        log_start_time = datetime.now()
        doc = xmltodict.parse(xml)

        # ---------------------------------------------------------------------------- #
        #                                Get Header Info                               #
        # ---------------------------------------------------------------------------- #
        source = doc["EventMessage"]["Header"]["Source"]
        msg_id = doc["EventMessage"]["Header"]["MessageID"]
        corr_id = doc["EventMessage"]["Header"]["CorrelationID"]
        msg_time = doc["EventMessage"]["Header"]["Timestamp"]
        verb = doc["EventMessage"]["Header"]["Verb"]
        noun = doc["EventMessage"]["Header"]["Noun"]
        context = doc["EventMessage"]["Header"]["Context"]
        rev = doc["EventMessage"]["Header"]["Revision"]
        read_group = doc["EventMessage"]["Header"]["Property"][0]["Value"]
        qos = doc["EventMessage"]["Header"]["Property"][1]["Value"]
        msg_idx = doc["EventMessage"]["Header"]["Property"][2]["Value"]

        header_tmp = {
            "source": source,
            "msg_id": msg_id,
            "corr_id": corr_id,
            "msg_time": msg_time,
            "read_group": read_group,
            "verb": verb,
            "noun": noun,
            "context": context,
            "msg_idx": msg_idx,
            "rev": rev,
            "qos": qos,
        }

        # print(header_tmp)

        # ---------------------------------------------------------------------------- #
        #                               Get Payload Info                               #
        # ---------------------------------------------------------------------------- #

        # Paramter init
        read_time = None
        reading_type = None
        rec_no = None
        meter_id = None
        interval = 0
        note = 0

        # Counter
        meters = {}
        columns = {}
        rt_count = 0

        # ------------------------------ 1.Meter Readings ----------------------------- #
        for meter_reading in doc["EventMessage"]["Payload"]["MeterReadings"]["MeterReading"]:
            if meter_reading["Meter"]["Names"]["NameType"]["name"] == "MeterUniqueID":
                meter_id = meter_reading["Meter"]["Names"]["name"]
                if meter_id not in meters.keys():
                    meters[meter_id] = 1

            # ------------------------------ 2.Interval Blocks ----------------------------- #
            for interval_block in meter_reading["IntervalBlocks"]:
                reading_type = LOADPROFILE[interval_block["ReadingType"]["@ref"]]["name"]  # 讀值欄位
                if reading_type != None:
                    rt_count += 1
                read_val = interval_block["IntervalReadings"]["value"]
                columns[reading_type] = read_val
                read_time = interval_block["IntervalReadings"]["timeStamp"]
                if read_time != None:
                    read_time = read_time[0:19]

                del_kwh = columns["del_kwh"] if "del_kwh" in columns else None
                rec_kwh = columns["rec_kwh"] if "rec_kwh" in columns else None
                del_kvarh_lag = columns["del_kvarh_lag"] if "del_kvarh_lag" in columns else None
                del_kvarh_lead = columns["del_kvarh_lead"] if "del_kvarh_lead" in columns else None
                rec_kvarh_lag = columns["rec_kvarh_lag"] if "rec_kvarh_lag" in columns else None
                rec_kvarh_lead = columns["rec_kvarh_lead"] if "rec_kvarh_lead" in columns else None

                # ---------------------------- 3.Reading Qualities --------------------------- #
                for reading_qualitie in interval_block["IntervalReadings"]["ReadingQualities"]:
                    if reading_qualitie["ReadingQualityType"]["@ref"] == "5.4.260":
                        rec_no = reading_qualitie["comment"] # 依據Reading Quality判斷是否為rec_no或者讀表狀態
                    elif reading_qualitie["ReadingQualityType"]["@ref"] == "1.5.257":
                        # ----------------------------------- error ---------------------------------- #
                        print("")
                    else:
                        reading_qualitie["ReadingQualityType"]["@ref"] != "5.4.260"
                        ref_code = reading_qualitie["ReadingQualityType"]["@ref"]
                        interval = QUALITYCODE[ref_code]["interval"]
                        note = QUALITYCODE[ref_code]["note"]

                meters[meter_id] += 1

                # LP 讀表物件
                read_map = {
                    "source": source,
                    "meter_id": meter_id,
                    "fan_id": "",
                    "rec_no": rec_no,
                    "read_time": read_time,
                    "interval": interval,
                    "note": note,
                    "del_kwh": del_kwh,
                    "rec_kwh": rec_kwh,
                    "del_kvarh_lag": del_kvarh_lag,
                    "del_kvarh_lead": del_kvarh_lead,
                    "rec_kvarh_lag": rec_kvarh_lag,
                    "rec_kvarh_lead": rec_kvarh_lead,
                    "version": 1,
                    "proc_type": 0,
                    "file_type": 'file_type',
                    "raw_gzfile": "",
                    "raw_file": 'file',
                    "rec_time": 'rec_time',
                    "file_path": 'path',
                    "file_size": "1789431",
                    "file_seqno": "1",
                    "msg_id": 'msg_id',
                    "corr_id": 'corr_id',
                    "msg_time": 'msg_time',
                    "rev": 'rev',
                    "qos": 'qos',
                    "rt_count": 'rt_count',
                }
        return read_map
    except Exception as e:
        print("2", e)
        exitcode = 1
    finally:
        print("Duration: {}".format(datetime.now() - log_start_time))
        sys.exit(exitcode)


pxml = '''
    <EventMessage xmlns="http://iec.ch/TC57/2011/schema/message">
      <Header>
        <Verb>created</Verb>
        <Noun>MeterReadings</Noun>
        <Revision>1</Revision>
        <Context>PRODUCTION</Context>
        <Timestamp>2022-11-18T08:42:21.539+08:00</Timestamp>
        <Source>HES-CHT20190919</Source>
        <MessageID>1cbd57c9-1ddc-42c2-bb22-07da17f6ad92</MessageID>
        <CorrelationID>1cbd57c9-1ddc-42c2-bb22-07da17f6ad92</CorrelationID>
        <Property>
          <Name>ReadingTypeGroup</Name>
          <Value>LP</Value>
        </Property>
        <Property>
          <Name>QOSLevel</Name>
          <Value>LEVEL-2</Value>
        </Property>
        <Property>
          <Name>MessageIndex</Name>
          <Value>1/1</Value>
        </Property>
      </Header>
      <Payload>
        <MeterReadings xmlns="http://iec.ch/TC57/2011/MeterReadings#">
          <MeterReading>
            <IntervalBlocks>
              <IntervalReadings>
                <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                <value>6519.0955</value>
                <ReadingQualities>
                  <source>HES</source>
                  <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="1.0.0"></ReadingQualityType>
                </ReadingQualities>
                <ReadingQualities>
                  <comment>4057</comment>
                  <source>HES</source>
                  <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="5.4.260"></ReadingQualityType>
                </ReadingQualities>
              </IntervalReadings>
              <ReadingType ref="0.0.2.9.1.2.12.0.0.0.0.0.0.0.0.3.72.0"></ReadingType>
            </IntervalBlocks>
            <IntervalBlocks>
              <IntervalReadings>
                <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                <value>0.8358</value>
                <ReadingQualities>
                  <source>HES</source>
                  <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="1.0.0"></ReadingQualityType>
                </ReadingQualities>
                <ReadingQualities>
                  <comment>4057</comment>
                  <source>HES</source>
                  <timeStamp>2022-11-18T06:45:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="5.4.260"></ReadingQualityType>
                </ReadingQualities>
              </IntervalReadings>
              <ReadingType ref="0.0.2.9.15.2.164.0.0.0.0.0.0.0.0.3.73.0"></ReadingType>
            </IntervalBlocks>
            <Meter>
              <mRID>WT20069447</mRID>
              <Names>
                <name>WT20069447</name>
                <NameType>
                  <name>MeterUniqueID</name>
                </NameType>
              </Names>
            </Meter>
          </MeterReading>
          <MeterReading>
            <IntervalBlocks>
              <IntervalReadings>
                <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                <value>6519.1396</value>
                <ReadingQualities>
                  <source>HES</source>
                  <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="1.0.0"></ReadingQualityType>
                </ReadingQualities>
                <ReadingQualities>
                  <comment>4058</comment>
                  <source>HES</source>
                  <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="5.4.260"></ReadingQualityType>
                </ReadingQualities>
              </IntervalReadings>
              <ReadingType ref="0.0.2.9.1.2.12.0.0.0.0.0.0.0.0.3.72.0"></ReadingType>
            </IntervalBlocks>
            <IntervalBlocks>
              <IntervalReadings>
                <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                <value>0.8358</value>
                <ReadingQualities>
                  <source>HES</source>
                  <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="1.0.0"></ReadingQualityType>
                </ReadingQualities>
                <ReadingQualities>
                  <comment>4058</comment>
                  <source>HES</source>
                  <timeStamp>2022-11-18T07:00:00.000+08:00</timeStamp>
                  <ReadingQualityType ref="5.4.260"></ReadingQualityType>
                </ReadingQualities>
              </IntervalReadings>
              <ReadingType ref="0.0.2.9.15.2.164.0.0.0.0.0.0.0.0.3.73.0"></ReadingType>
            </IntervalBlocks>
            <Meter>
              <mRID>WT20069447</mRID>
              <Names>
                <name>WT20069447</name>
                <NameType>
                  <name>MeterUniqueID</name>
                </NameType>
              </Names>
            </Meter>
          </MeterReading>
        </MeterReadings>
      </Payload>
    </EventMessage>
'''


parse_xml(pxml)
