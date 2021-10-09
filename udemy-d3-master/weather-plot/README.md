A modification of Susie Lu's radial weather plot that shows cloudiness, rainy days and days when the temperature dipped below freezing.

Her original readme:

## Weather Plot - New York 2015

In the example we're looking at historical weather data for New York provided by [intellicast.com](http://www.intellicast.com/) and [wunderground.com](http://www.wunderground.com/). Inspired by [weather-radicals.com](http://www.weather-radials.com/).

This example uses scales to roll your own radial projection by mapping out the x, y, and r positions. If you are creating a line or an area you can use d3's convenience functions d3.svg.line.radial and d3.svg.area.radial but this is a method you can use if you want to use different graphical elements in a circular layout.