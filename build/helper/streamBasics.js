import * as tslib_1 from "tslib";
import { Readable } from 'readable-stream';
var ReadableError = (function (_super) {
    tslib_1.__extends(ReadableError, _super);
    function ReadableError(message, options) {
        if (options === void 0) { options = {}; }
        var _this = _super.call(this, options) || this;
        var err = typeof message === 'string' ? new Error(message) : message;
        setTimeout(function () {
            _this.emit('error', err);
        }, 1);
        return _this;
    }
    ;
    ReadableError.prototype._read = function () {
    };
    return ReadableError;
}(Readable));
export { ReadableError };
