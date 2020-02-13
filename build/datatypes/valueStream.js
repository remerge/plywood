import { Dataset } from './dataset';
export function iteratorFactory(value) {
    if (value instanceof Dataset)
        return datasetIteratorFactory(value);
    var nextBit = { type: 'value', value: value };
    return function () {
        var ret = nextBit;
        nextBit = null;
        return ret;
    };
}
export function datasetIteratorFactory(dataset) {
    var curRowIndex = -2;
    var curRow = null;
    var cutRowDatasets = [];
    function nextSelfRow() {
        curRowIndex++;
        cutRowDatasets = [];
        var row = dataset.data[curRowIndex];
        if (row) {
            curRow = {};
            for (var k in row) {
                var v = row[k];
                if (v instanceof Dataset) {
                    cutRowDatasets.push({
                        attribute: k,
                        datasetIterator: datasetIteratorFactory(v)
                    });
                }
                else {
                    curRow[k] = v;
                }
            }
        }
        else {
            curRow = null;
        }
    }
    return function () {
        if (curRowIndex === -2) {
            curRowIndex++;
            var initEvent = {
                type: 'init',
                attributes: dataset.attributes
            };
            if (dataset.keys.length)
                initEvent.keys = dataset.keys;
            return initEvent;
        }
        var pb;
        while (cutRowDatasets.length && !pb) {
            pb = cutRowDatasets[0].datasetIterator();
            if (!pb)
                cutRowDatasets.shift();
        }
        if (pb) {
            return {
                type: 'within',
                attribute: cutRowDatasets[0].attribute,
                within: pb
            };
        }
        nextSelfRow();
        return curRow ? {
            type: 'datum',
            datum: curRow
        } : null;
    };
}
var PlywoodValueBuilder = (function () {
    function PlywoodValueBuilder() {
        this._value = null;
        this._curAttribute = null;
        this._curValueBuilder = null;
    }
    PlywoodValueBuilder.prototype._finalizeLastWithin = function () {
        if (!this._curValueBuilder)
            return;
        var lastDatum = this._data[this._data.length - 1];
        if (!lastDatum)
            throw new Error('unexpected within');
        lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
        this._curAttribute = null;
        this._curValueBuilder = null;
    };
    PlywoodValueBuilder.prototype.processBit = function (bit) {
        if (typeof bit !== 'object')
            throw new Error("invalid bit: " + bit);
        switch (bit.type) {
            case 'value':
                this._value = bit.value;
                this._data = null;
                this._curAttribute = null;
                this._curValueBuilder = null;
                break;
            case 'init':
                this._finalizeLastWithin();
                this._attributes = bit.attributes;
                this._keys = bit.keys;
                this._data = [];
                break;
            case 'datum':
                this._finalizeLastWithin();
                if (!this._data)
                    this._data = [];
                this._data.push(bit.datum);
                break;
            case 'within':
                if (!this._curValueBuilder) {
                    this._curAttribute = bit.attribute;
                    this._curValueBuilder = new PlywoodValueBuilder();
                }
                this._curValueBuilder.processBit(bit.within);
                break;
            default:
                throw new Error("unexpected type: " + bit.type);
        }
    };
    PlywoodValueBuilder.prototype.getValue = function () {
        var _data = this._data;
        if (_data) {
            if (this._curValueBuilder) {
                var lastDatum = _data[_data.length - 1];
                if (!lastDatum)
                    throw new Error('unexpected within');
                lastDatum[this._curAttribute] = this._curValueBuilder.getValue();
            }
            return new Dataset({
                attributes: this._attributes,
                keys: this._keys,
                data: _data
            });
        }
        else {
            return this._value;
        }
    };
    return PlywoodValueBuilder;
}());
export { PlywoodValueBuilder };
