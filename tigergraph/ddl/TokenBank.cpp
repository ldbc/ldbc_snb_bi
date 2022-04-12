
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <TokenLib.hpp>

extern "C" int64_t ToMiliSeconds(const char* const iToken[], uint32_t iTokenLen[], uint32_t iTokenNum) {
  struct tm t;
  memset(&t, 0, sizeof(struct tm));
  strptime(iToken[0], "%Y-%m-%dT%H:%M:%S", &t);
  t.tm_isdst = -1;
  time_t epoch_t = mktime(&t);// this is local time in your computer
  int ms = 0;
  if (iTokenLen[0] > 20 ) ms = atoi(iToken[0] + 20);
  return (uint64_t)(epoch_t * 1000 + ms);
}
