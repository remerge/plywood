import { DruidFilterBuilder } from './druidFilterBuilder';
var DruidHavingFilterBuilder = (function () {
    function DruidHavingFilterBuilder(options) {
        this.version = options.version;
        this.attributes = options.attributes;
        this.customTransforms = options.customTransforms;
    }
    DruidHavingFilterBuilder.prototype.filterToHavingFilter = function (filter) {
        return {
            type: 'filter',
            filter: new DruidFilterBuilder({
                version: this.version,
                rawAttributes: this.attributes,
                timeAttribute: 'z',
                allowEternity: true,
                customTransforms: this.customTransforms
            }).timelessFilterToFilter(filter)
        };
    };
    return DruidHavingFilterBuilder;
}());
export { DruidHavingFilterBuilder };
