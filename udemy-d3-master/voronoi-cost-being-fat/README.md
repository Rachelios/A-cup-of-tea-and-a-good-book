This [block](https://bl.ocks.org/Kcnarf/238fa136f763f5ad908271a170ef60e2) illustrates the use of the [d3-voronoi-map](https://github.com/Kcnarf/d3-voronoi-map) plugin. This block is a remake of the [HowMuch.net](https://howmuch.net)'s post [The Costs of Being Fat, in Actual Dollars](https://howmuch.net/articles/obesity-costs-visualized).

The [d3-voronoi-map](https://github.com/Kcnarf/d3-voronoi-treemap) plugin produces Voronoï maps (one-level treemap). Given a convex polygon (here, a 60-gon simulating a circle for each gender) and weighted data, it tesselates/partitions the polygon in several inner cells, such that the area of a cell represents the weight of the underlying datum.

[An iteration](https://bl.ocks.org/Kcnarf/e649c8723eff3fd64a23f75901910930) on this block enhances the user experience by (i) always producing the same Voronoï map on reloads and (ii) having the same layout (e.g. placing sites/cells of the same type at the same position) which eases comparison.

#### Acknowledgments to :
* [D3.js](https://d3js.org/) (v.4)
* [d3-voronoi-map](https://github.com/Kcnarf/d3-voronoi-map) plugin
* [blockbuilder.org](http://blockbuilder.org)
* (part of) [https://github.com/ArlindNocaj/Voronoi-Treemap-Library](https://github.com/ArlindNocaj/Voronoi-Treemap-Library) for a Java implementation