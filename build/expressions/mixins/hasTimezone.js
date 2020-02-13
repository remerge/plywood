import { Timezone } from 'chronoshift';
import { Expression } from '../baseExpression';
var HasTimezone = (function () {
    function HasTimezone() {
    }
    HasTimezone.prototype.getTimezone = function () {
        return this.timezone || Timezone.UTC;
    };
    HasTimezone.prototype.changeTimezone = function (timezone) {
        if (timezone.equals(this.timezone))
            return this;
        var value = this.valueOf();
        value.timezone = timezone;
        return Expression.fromValue(value);
    };
    HasTimezone.prototype.needsEnvironment = function () {
        return !this.timezone;
    };
    HasTimezone.prototype.defineEnvironment = function (environment) {
        if (!environment.timezone)
            environment = { timezone: Timezone.UTC };
        if (typeof environment.timezone === 'string')
            environment = { timezone: Timezone.fromJS(environment.timezone) };
        if (this.timezone || !environment.timezone)
            return this;
        return this.changeTimezone(environment.timezone).substitute(function (ex) {
            if (ex.needsEnvironment()) {
                return ex.defineEnvironment(environment);
            }
            return null;
        });
    };
    return HasTimezone;
}());
export { HasTimezone };
