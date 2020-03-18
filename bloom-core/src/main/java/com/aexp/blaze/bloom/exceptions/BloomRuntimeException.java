/**
 *
 *   Copyright 2020 American Express Travel Related Services Company, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the License
 *   is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *   or implied. See the License for the specific language governing permissions and limitations under
 *   the License.
 */

package com.aexp.blaze.bloom.exceptions;

import com.aexp.blaze.bloom.constants.ErrorCodes;

public class BloomRuntimeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final ErrorCodes error;

    public BloomRuntimeException(String msg) {
        super(msg);
        error = ErrorCodes.MBL_DEFAULT;
    }

    public BloomRuntimeException(String msg, ErrorCodes e) {
        super(msg);
        this.error = e;
    }

    public BloomRuntimeException(String msg, Throwable e) {
        super(msg, e);
        error = null;
    }

    public BloomRuntimeException(String msg, ErrorCodes e, Throwable t) {
        super(msg, t);
        error = e;
    }

    @Override
    public String getMessage() {
        return "error code:" + error + ", " + super.getMessage();
    }
}
