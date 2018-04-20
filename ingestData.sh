# ingestData.sh takes a file of paths to curl and downloads them to the directory where it is called from
#!/bin/bash

runDir="UPDATE ME!!"

while read filePath;
do
  fileDir=`dirname $filePath`
  fullDir=$runDir$fileDir
  if [ -d "$fullDir" ];
  then
    echo "Directory exists";
    source="https://commoncrawl.s3.amazonaws.com/$filePath";
    echo "Source file: $source";
    destination=$runDir$filePath
    echo "Destination: $destination";
    curl $source > $destination;
  else
    echo "Directory does not exist, creating directory:";
    echo $fullDir;
    mkdir -p $fullDir;
    echo "Directory created.";
    echo "Ingesting file."
    source="https://commoncrawl.s3.amazonaws.com/$filePath";
    echo "Source file: $source";
    destination=$runDir$filePath
    echo "Destination: $destination";
    curl $source > $destination;
  fi;
done;
