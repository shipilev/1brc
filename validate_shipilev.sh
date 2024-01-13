#!/bin/bash
#
#  Copyright 2023 The original authors
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

set -euo pipefail

for sample in $(ls src/test/resources/samples/*.txt); do
  echo "Validating $sample"

  ln -sf $sample measurements.txt
  hexdump -C ${sample%.txt}.out > sample.hex

  ./calculate_average_shipilev.sh > actual.out
  hexdump -C actual.out > actual.hex
  diff -uwb sample.hex actual.hex
done

echo "Validating measurements1.txt"
ln -sf measurements1.txt measurements.txt
./calculate_average_shipilev.sh > actual-1.out
hexdump -C actual-1.out > actual-1.hex
#java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_baseline > baseline-1.out
hexdump -C baseline-1.out > baseline-1.hex
diff -uwb baseline-1.hex actual-1.hex

echo "Validating measurements2.txt"
ln -sf measurements2.txt measurements.txt
./calculate_average_shipilev.sh > actual-2.out
hexdump -C actual-2.out > actual-2.hex
#java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_baseline > baseline-2.out
hexdump -C baseline-2.out > baseline-2.hex
diff -uwb baseline-2.hex actual-2.hex

echo "Validating measurements3.txt"
ln -sf measurements3.txt measurements.txt
./calculate_average_shipilev.sh > actual-3.out
hexdump -C actual-3.out > actual-3.hex
#java --class-path target/average-1.0.0-SNAPSHOT.jar dev.morling.onebrc.CalculateAverage_baseline > baseline-3.out
hexdump -C baseline-3.out > baseline-3.hex
diff -uwb baseline-3.hex actual-3.hex

echo "All valid"

ln -sf measurements2.txt measurements.txt
