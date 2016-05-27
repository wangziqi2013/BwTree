#pragma once
#ifndef __COMMON_TIME_MEASURER_H__
#define __COMMON_TIME_MEASURER_H__

#include <chrono>
using std::chrono::high_resolution_clock;
using std::chrono::system_clock;
using std::chrono::milliseconds;
using std::chrono::microseconds;
using std::chrono::nanoseconds;

class TimeMeasurer{
public:
  TimeMeasurer(){}
  ~TimeMeasurer(){}

  void StartTimer(){
    start_time_ = high_resolution_clock::now();
  }

  void EndTimer(){
    end_time_ = high_resolution_clock::now();
  }

  long long GetElapsedMilliSeconds(){
    return std::chrono::duration_cast<milliseconds>(end_time_ - start_time_).count();
  }

  long long GetElapsedMicroSeconds(){
    return std::chrono::duration_cast<microseconds>(end_time_ - start_time_).count();
  }

  long long GetElapsedNanoSeconds(){
    return std::chrono::duration_cast<nanoseconds>(end_time_ - start_time_).count();
  }

  static system_clock::time_point GetTimePoint() {
    return high_resolution_clock::now();
  }

  static long long CalcMilliSecondDiff(system_clock::time_point &start, system_clock::time_point &end) {
    return std::chrono::duration_cast<milliseconds>(end - start).count();
  }

private:
  TimeMeasurer(const TimeMeasurer&);
  TimeMeasurer& operator=(const TimeMeasurer&);

private:
  system_clock::time_point start_time_;
  system_clock::time_point end_time_;
};

#endif