#!/bin/bash
# TestBasic TestBfsFind TestBfsFindAll1 TestBfsFindAll2 TestBfsFindAll3 TestRandomWalkFindAll TestRandomWalkFind TestHashAndEqual TestStateInherit TestNextStates TestPartition
for t in TestHashAndEqual TestStateInherit TestNextStates TestPartition # add all the test names here
do
  echo $t
  count=0
  n=50
  for i in $(seq 1 $n)
  do
    go test -run "^${t}$" -timeout 2m > ./log-${t}-${i}.txt
    result=$(grep -E '^PASS$' log-${t}-${i}.txt| wc -l)
    count=$((count + result))
    if [ $result -eq 1 ]; then
       rm ./log-${t}-${i}.txt
    fi
  done
  echo "$count/$n"
done

