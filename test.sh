#!/bin/bash
cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export GOPATH=$cur_dir
cd $cur_dir

case $1 in
    "lab1")
        cd src/mapreduce
        ;;
    "lab2")
        cd src/raft
        ;;
    *)
        echo "unknown lab $1"
        exit
        ;;
esac

shift

tmpfile=$(mktemp /tmp/abc-script.XXXXXX)

test_name=""
num_loop=10

if [ "$#" -eq 1 ]; then
    re='^[0-9]+$'
    if ! [[ $1 =~ $re ]] ; then
        test_name=$1
    else
        num_loop=$1
    fi
elif [ "$#" -ge 2 ]; then
    test_name=$1
    num_loop=$2
fi

echo "test name: $test_name"
echo "loop $num_loop times"

for ((i=0;i<$num_loop;i++))
do
    echo "========== $i th test begin =========="
    if [ -z "$test_name" ]; then
        go test | tee -a $tmpfile
    else
        go test -run $test_name | tee -a $tmpfile
    fi
done

pass=`grep ^PASS$ $tmpfile | wc -l`
failed=`expr $num_loop - $pass`
echo
echo "========== result =========="
echo
echo "success: $pass"
echo "fail: $failed"
echo
echo "========== end =========="
rm "$tmpfile"

echo

if [ "$pass" != "$num_loop" ]; then
    echo "tests failed."
    exit 1
else
    echo "tests success."
    exit 0
fi
