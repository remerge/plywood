import * as tslib_1 from "tslib";
import { PassThrough } from 'readable-stream';
var StreamConcat = (function (_super) {
    tslib_1.__extends(StreamConcat, _super);
    function StreamConcat(options) {
        var _this = _super.call(this, options) || this;
        _this.next = options.next;
        _this.currentStream = null;
        _this.streamIndex = 0;
        _this._nextStream();
        return _this;
    }
    ;
    StreamConcat.prototype._nextStream = function () {
        var _this = this;
        this.currentStream = null;
        this.currentStream = this.next();
        if (this.currentStream == null) {
            this.push(null);
        }
        else {
            this.currentStream.pipe(this, { end: false });
            this.currentStream.on('error', function (e) { return _this.emit('error', e); });
            this.currentStream.on('end', this._nextStream.bind(this));
        }
    };
    return StreamConcat;
}(PassThrough));
export { StreamConcat };
