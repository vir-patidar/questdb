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

package io.questdb.griffin.engine.functions.cast;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.constants.UuidConstant;
import io.questdb.std.*;
import io.questdb.std.str.Utf8Sequence;

public class CastVarcharToUuidFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "cast(Øz)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function arg = args.getQuick(0);
        if (arg.isConstant()) {
            final Utf8Sequence value = arg.getVarcharA(null);
            if (value == null || value.size() == 0) {
                return UuidConstant.NULL;
            }
            final Uuid uuid = new Uuid();
            try {
                uuid.of(value);
            } catch (NumericException e) {
                throw SqlException.$(argPositions.getQuick(0), "invalid UUID constant");
            }
            return new UuidConstant(uuid);
        }
        return new Func(arg);
    }

    public static class Func extends AbstractCastToUuidFunction {
        public Func(Function arg) {
            super(arg);
        }

        @Override
        public long getLong128Hi(Record rec) {
            final Utf8Sequence value = arg.getVarcharA(rec);
            if (value == null) {
                return Numbers.LONG_NaN;
            }
            try {
                Uuid.checkDashesAndLength(value.asAsciiCharSequence());
                return Uuid.parseHi(value.asAsciiCharSequence());
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }

        @Override
        public long getLong128Lo(Record rec) {
            final Utf8Sequence value = arg.getVarcharA(rec);
            if (value == null) {
                return Numbers.LONG_NaN;
            }
            try {
                Uuid.checkDashesAndLength(value.asAsciiCharSequence());
                return Uuid.parseLo(value.asAsciiCharSequence());
            } catch (NumericException e) {
                return Numbers.LONG_NaN;
            }
        }
    }
}
