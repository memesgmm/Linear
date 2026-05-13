#!/bin/bash
set -e

mkdir -p output_jars

echo "Building Legacy (1.21.1) JAR..."
./gradlew clean jar -PbuildTarget=legacy
cp build/libs/linear-1.21.1-*.jar ./output_jars/linear-legacy.jar

echo "Building Modern (26.1.2) JAR..."
./gradlew clean jar -PbuildTarget=modern
cp build/libs/linear-26*.jar ./output_jars/linear-modern.jar

echo "Build complete."
ls -l output_jars/
