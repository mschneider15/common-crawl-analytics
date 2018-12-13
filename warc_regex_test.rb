#!/usr/bin/env ruby
require 'warc'

# milEmailPattern = /\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9.-]{1,63}\.){1,125}mil\b/
milEmailPattern = /\b[A-Za-z0-9._%+-]{1,64}@(?:[A-Za-z0-9-\.]{1,63}\.)mil\b/
input_stream = File.open("/Users/ethan/common-crawl-analytics/CC-MAIN-20181112172845-20181112194419-00015.warc.wet")

msg_num = 0
match_numbers = []
timings = []
while(record = Warc::Parser.new.parse(input_stream)) do
  matches = 0
  t1 = Time.now
  if msg_num == 44092
    puts record.content.length
    File.open("bad_file.warc","w") do |ofh|
      ofh.write record.content
    end
  end
  record.content.each_line do | line |
    if msg_num == 44092
      $stderr.puts line
    end
    if m = line.match(milEmailPattern)
      matches += 1
    end
  end
  t2 = Time.now
  timings << t2 - t1

  $stderr.puts "#{msg_num} #{t2 - t1} #{matches}"
  msg_num += 1
  if matches > 0
    match_numbers << matches
  end
end  
puts match_numbers.sort.last(20)


__END__
[ethan@ethans-mbp ~/common-crawl-analytics]$ grep "Content-Length" CC-MAIN-20181112172845-20181112194419-00015.warc.wet | sort -nr -k 2 | head -n 50
Content-Length: 1361988
Content-Length: 770782
Content-Length: 723565
Content-Length: 632602
Content-Length: 631658
Content-Length: 557306
Content-Length: 557305
Content-Length: 557206
Content-Length: 505460
Content-Length: 475424
Content-Length: 458704
Content-Length: 436921
Content-Length: 436827
Content-Length: 401117
Content-Length: 393951
Content-Length: 392614
Content-Length: 371811
Content-Length: 371328
Content-Length: 371093
Content-Length: 371074
Content-Length: 370668
Content-Length: 370668
Content-Length: 370579
Content-Length: 370464
Content-Length: 370150
Content-Length: 370114
Content-Length: 370046
Content-Length: 370046
Content-Length: 369657
Content-Length: 369652
Content-Length: 369213
Content-Length: 368179
Content-Length: 367478
Content-Length: 350011
Content-Length: 329284
Content-Length: 329284
Content-Length: 329284
Content-Length: 327501
Content-Length: 327501
Content-Length: 314811
Content-Length: 314652
Content-Length: 314638
Content-Length: 314345
Content-Length: 311864
Content-Length: 301626
Content-Length: 294492
Content-Length: 294492
Content-Length: 290559
Content-Length: 289639
Content-Length: 289639
[ethan@ethans-mbp ~/common-crawl-analytics]$ grep "Content-Length" CC-MAIN-20181112172845-20181112194419-00015.warc.wet | wc -l 
   47719
