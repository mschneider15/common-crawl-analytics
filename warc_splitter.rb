#!/usr/bin/env ruby
require 'warc'

input_stream = File.open("/Users/ethan/common-crawl-analytics/CC-MAIN-20181112172845-20181112194419-00015.warc.wet")

msg_num = 0
output_num = 0
output_stream = nil
while(record = Warc::Parser.new.parse(input_stream)) do

  if msg_num % 2000 == 0
    output_stream.close unless output_stream.nil?
    output_stream = File.open("/Users/ethan/common-crawl-analytics/warc_files/msg_#{sprintf '%04d',output_num}.warc","w")
    output_num += 1
  end
  
  record.dump_to(output_stream)
  msg_num += 1
end
output_stream.close
