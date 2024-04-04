/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb;

import io.questdb.cairo.SecurityContext;
import io.questdb.log.Log;
import io.questdb.log.LogRecord;

public interface QueryLogger {
    // called when an empty query received
    default void logEmptyQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "empty query");
    }

    // called when a cached query executed
    default void logExecQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "exec");
    }

    // called when a new query parsed
    default void logParseQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext) {
        logQuery(logger, doLog, fd, query, securityContext, "parse");
    }

    LogRecord logQuery(Log logger, int fd, CharSequence query, SecurityContext securityContext, String logText);

    default void logQuery(Log logger, boolean doLog, int fd, CharSequence query, SecurityContext securityContext, String logText) {
        if (doLog) {
            logQuery(logger, fd, query, securityContext, logText).I$();
        }
    }
}
