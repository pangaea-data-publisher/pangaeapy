# Documentation
<main>
<article id="content">
<header>
<h1 class="title"><code>pangaeapy/pandataset</code> module</h1>
</header>
<section id="section-intro">
<p>Created on Tue Aug 21 13:31:30 2018</p>
<p>@author: Robert Huber
@author: Markus Stocker</p>
</section>
<section>
</section>
<section>
</section>
<section>
</section>
<section>
<h2 class="section-title" id="header-classes">Classes</h2>
<dl>
<dt id="pangaeapy/pandataset.PanAuthor"><code class="flex name class">
<span>class <span class="ident">PanAuthor</span></span>
</code></dt>
<dd>
<section class="desc"><p>PANGAEA Author Class.
A simple helper class to declare 'author' objects which are associated as part of the metadata of a given PANGAEA dataset object</p>
<h2 id="parameters">Parameters</h2>
<dl>
<dt><strong><code>lastname</code></strong> :&ensp;<code>str</code></dt>
<dd>The author's first name</dd>
<dt><strong><code>firstname</code></strong> :&ensp;<code>str</code></dt>
<dd>The authors's last name</dd>
</dl>
<h2 id="attributes">Attributes</h2>
<dl>
<dt><strong><code>lastname</code></strong> :&ensp;<code>str</code></dt>
<dd>The author's first name</dd>
<dt><strong><code>firstname</code></strong> :&ensp;<code>str</code></dt>
<dd>The authors's last name</dd>
<dt><strong><code>fullname</code></strong> :&ensp;<code>str</code></dt>
<dd>Combination of lastname, firstname. This attribute is created by the constructor</dd>
</dl></section>
<h3>Methods</h3>
<dl>
<dt id="pangaeapy/pandataset.PanAuthor.__init__"><code class="name flex">
<span>def <span class="ident">__init__</span></span>(<span>self, lastname, firstname=None)</span>
</code></dt>
<dd>
<section class="desc"><p>Initialize self.
See help(type(self)) for accurate signature.</p></section>
</dd>
</dl>
</dd>
<dt id="pangaeapy/pandataset.PanDataSet"><code class="flex name class">
<span>class <span class="ident">PanDataSet</span></span>
</code></dt>
<dd>
<section class="desc"><p>PANGAEA DataSet
The PANGAEA PanDataSet class enables the creation of objects which hold the necessary information, including data as well as metadata, to analyse a given PANGAEA dataset.</p>
<h2 id="parameters">Parameters</h2>
<dl>
<dt><strong><code>id</code></strong> :&ensp;<code>str</code></dt>
<dd>The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here</dd>
<dt><strong><code>deleteFlag</code></strong> :&ensp;<code>str</code></dt>
<dd>in case quality flags are avialable, this parameter defines a flag for which data should not be included in the data dataFrame.
Possible values are listed here: https://wiki.pangaea.de/wiki/Quality_flag</dd>
</dl>
<h2 id="attributes">Attributes</h2>
<dl>
<dt><strong><code>id</code></strong> :&ensp;<code>str</code></dt>
<dd>The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here</dd>
<dt><strong><code>uri</code></strong> :&ensp;<code>str</code></dt>
<dd>The PANGAEA DOI</dd>
<dt><strong><code>title</code></strong> :&ensp;<code>str</code></dt>
<dd>The title of the dataset</dd>
<dt><strong><code>year</code></strong> :&ensp;<code>int</code></dt>
<dd>The publication year of teh dataset</dd>
<dt><strong><code>authors</code></strong> :&ensp;<code>list</code> of <a title="pangaeapy/pandataset.PanAuthor" href="#pangaeapy/pandataset.PanAuthor"><code>PanAuthor</code></a></dt>
<dd>a list containing the PanAuthot objects (author info) of the dataset</dd>
<dt><strong><code>citation</code></strong> :&ensp;<code>str</code></dt>
<dd>the full citation of the dataset including e.g. author, year, title etc..</dd>
<dt><strong><code>params</code></strong> :&ensp;<code>list</code> of <a title="pangaeapy/pandataset.PanParam" href="#pangaeapy/pandataset.PanParam"><code>PanParam</code></a></dt>
<dd>a list of all PanParam objects (the parameters) used in this dataset</dd>
<dt><strong><code>events</code></strong> :&ensp;<code>list</code> of <a title="pangaeapy/pandataset.PanEvent" href="#pangaeapy/pandataset.PanEvent"><code>PanEvent</code></a></dt>
<dd>a list of all PanEvent objects (the events) used in this dataset</dd>
<dt><strong><code>data</code></strong> :&ensp;<code>pandas.DataFrame</code></dt>
<dd>a pandas dataframe holding all the data</dd>
<dt><strong><code>loginstatus</code></strong> :&ensp;<code>str</code></dt>
<dd>a label which indicates if the data set is protected or not default value: 'unrestricted'</dd>
<dt><strong><code>isParent</code></strong> :&ensp;<code>boolean</code></dt>
<dd>indicates if this dataset is a parent data set within a collection of child data sets</dd>
</dl></section>
<h3>Methods</h3>
<dl>
<dt id="pangaeapy/pandataset.PanDataSet.__init__"><code class="name flex">
<span>def <span class="ident">__init__</span></span>(<span>self, id=None, deleteFlag=&#39;&#39;)</span>
</code></dt>
<dd>
<section class="desc"><p>Initialize self.
See help(type(self)) for accurate signature.</p></section>
</dd>
<dt id="pangaeapy/pandataset.PanDataSet.setData"><code class="name flex">
<span>def <span class="ident">setData</span></span>(<span>self, addEventColumns=True)</span>
</code></dt>
<dd>
<section class="desc"><p>This method populates the data DataFrame with data from a PANGAEA dataset.
In addition to the data given in the tabular ASCII file delivered by PANGAEA.</p>
<h2 id="parameters">Parameters:</h2>
<dl>
<dt><strong><code>addEventColumns</code></strong> :&ensp;<code>boolean</code></dt>
<dd>In case Latitude, Longititude, Elevation, Date/Time and Event are not given in the ASCII matrix, which sometimes is possible in single Event datasets,
the setData could add these columns to the dataframe using the information given in the metadata for Event. Default is 'True'</dd>
</dl></section>
</dd>
<dt id="pangaeapy/pandataset.PanDataSet.setID"><code class="name flex">
<span>def <span class="ident">setID</span></span>(<span>self, id)</span>
</code></dt>
<dd>
<section class="desc"><p>Initialize the ID of a data set in case it was not defined in the constructur
Parameters</p>
<hr>
<dl>
<dt><strong><code>id</code></strong> :&ensp;<code>str</code></dt>
<dd>The identifier of a PANGAEA dataset. An integer number or a DOI is accepted here</dd>
</dl></section>
</dd>
<dt id="pangaeapy/pandataset.PanDataSet.setMetadata"><code class="name flex">
<span>def <span class="ident">setMetadata</span></span>(<span>self)</span>
</code></dt>
<dd>
<section class="desc"><p>The method initializes the metadata of the PanDataSet object using the information of a PANGAEA metadata XML file.</p></section>
</dd>
</dl>
</dd>
<dt id="pangaeapy/pandataset.PanEvent"><code class="flex name class">
<span>class <span class="ident">PanEvent</span></span>
</code></dt>
<dd>
<section class="desc"><p>PANGAEA Event Class.
An Event can be regarded as a named entity which is defined by the usage of a distinct method or device at a distinct location during a given time interval for scientific purposes.
More infos on PANGAEA's Evenmts can be found here: https://wiki.pangaea.de/wiki/Event</p>
<h2 id="parameters">Parameters</h2>
<dl>
<dt><strong><code>label</code></strong> :&ensp;<code>str</code></dt>
<dd>A label which is used to name the event</dd>
<dt><strong><code>latitude</code></strong> :&ensp;<code>float</code></dt>
<dd>The latitude of the event location</dd>
<dt><strong><code>longitude</code></strong> :&ensp;<code>float</code></dt>
<dd>The longitude of the event location</dd>
<dt><strong><code>elevation</code></strong> :&ensp;<code>float</code></dt>
<dd>The elevation (relative to sea level) of the event location</dd>
<dt><strong><code>datetime</code></strong> :&ensp;<code>str</code></dt>
<dd>The date and time of the event in ´%Y/%m/%dT%H:%M:%S´ format</dd>
<dt><strong><code>device</code></strong> :&ensp;<code>str</code></dt>
<dd>The device which was used during the event</dd>
</dl>
<h2 id="attributes">Attributes</h2>
<dl>
<dt><strong><code>label</code></strong> :&ensp;<code>str</code></dt>
<dd>A label which is used to name the event</dd>
<dt><strong><code>latitude</code></strong> :&ensp;<code>float</code></dt>
<dd>The latitude of the event location</dd>
<dt><strong><code>longitude</code></strong> :&ensp;<code>float</code></dt>
<dd>The longitude of the event location</dd>
<dt><strong><code>elevation</code></strong> :&ensp;<code>float</code></dt>
<dd>The elevation (relative to sea level) of the event location</dd>
<dt><strong><code>datetime</code></strong> :&ensp;<code>str</code></dt>
<dd>The date and time of the event in ´%Y/%m/%dT%H:%M:%S´ format</dd>
<dt><strong><code>device</code></strong> :&ensp;<code>str</code></dt>
<dd>The device which was used during the event</dd>
</dl></section>
<h3>Methods</h3>
<dl>
<dt id="pangaeapy/pandataset.PanEvent.__init__"><code class="name flex">
<span>def <span class="ident">__init__</span></span>(<span>self, label, latitude, longitude, elevation=None, datetime=None, device=None)</span>
</code></dt>
<dd>
<section class="desc"><p>Initialize self.
See help(type(self)) for accurate signature.</p></section>
</dd>
</dl>
</dd>
<dt id="pangaeapy/pandataset.PanParam"><code class="flex name class">
<span>class <span class="ident">PanParam</span></span>
</code></dt>
<dd>
<section class="desc"><p>PANGAEA Parameter
Shoud be used to create PANGAEA parameter objects. Parameter is used here to represent 'measured variables'</p>
<h2 id="parameters">Parameters</h2>
<dl>
<dt><strong><code>id</code></strong> :&ensp;<code>int</code></dt>
<dd>the identifier for the parameter</dd>
<dt><strong><code>name</code></strong> :&ensp;<code>str</code></dt>
<dd>A long name or title used for the parameter</dd>
<dt><strong><code>shortName</code></strong> :&ensp;<code>str</code></dt>
<dd>A short name or label to identify the parameter</dd>
<dt><strong><code>param_type</code></strong> :&ensp;<code>str</code></dt>
<dd>indicates the data type of the parameter (string, numeric, datetime etc..)</dd>
<dt><strong><code>source</code></strong> :&ensp;<code>str</code></dt>
<dd>defines the category or source for a parameter (e.g. geocode, data, event)&hellip; very PANGAEA specific ;)</dd>
<dt><strong><code>unit</code></strong> :&ensp;<code>str</code></dt>
<dd>the unit of measurement used with this parameter (e.g. m/s, kg etc..)</dd>
</dl>
<h2 id="attributes">Attributes</h2>
<dl>
<dt><strong><code>id</code></strong> :&ensp;<code>int</code></dt>
<dd>the identifier for the parameter</dd>
<dt><strong><code>name</code></strong> :&ensp;<code>str</code></dt>
<dd>A long name or title used for the parameter</dd>
<dt><strong><code>shortName</code></strong> :&ensp;<code>str</code></dt>
<dd>A short name or label to identify the parameter</dd>
<dt><strong><code>synonym</code></strong> :&ensp;<code>dict</code></dt>
<dd>A diconary of synonyms for the parameter whcih e.g. is used by other archives or communities.
The dict key indicates the namespace (possible values currently are CF and OS)</dd>
<dt><strong><code>type</code></strong> :&ensp;<code>str</code></dt>
<dd>indicates the data type of the parameter (string, numeric, datetime etc..)</dd>
<dt><strong><code>source</code></strong> :&ensp;<code>str</code></dt>
<dd>defines the category or source for a parameter (e.g. geocode, data, event)&hellip; very PANGAEA specific ;)</dd>
<dt><strong><code>unit</code></strong> :&ensp;<code>str</code></dt>
<dd>the unit of measurement used with this parameter (e.g. m/s, kg etc..)</dd>
</dl></section>
<h3>Methods</h3>
<dl>
<dt id="pangaeapy/pandataset.PanParam.__init__"><code class="name flex">
<span>def <span class="ident">__init__</span></span>(<span>self, id, name, shortName, param_type, source, unit=None)</span>
</code></dt>
<dd>
<section class="desc"><p>Initialize self.
See help(type(self)) for accurate signature.</p></section>
</dd>
<dt id="pangaeapy/pandataset.PanParam.addSynonym"><code class="name flex">
<span>def <span class="ident">addSynonym</span></span>(<span>name, ns)</span>
</code></dt>
<dd>
<section class="desc"><p>Creates a new synonym for a parameter which is valid within the given name space. Synonyms are stored in the synonym attribute which is a dictionary</p>
<h2 id="parameters">Parameters</h2>
<dl>
<dt><strong><code>name</code></strong> :&ensp;<code>str</code></dt>
<dd>the name of the synonym</dd>
<dt><strong><code>ns</code></strong> :&ensp;<code>str</code></dt>
<dd>the namespace indicator for the sysnonym</dd>
</dl></section>
</dd>
</dl>
</dd>
</dl>
</section>
</article>
<nav id="sidebar">
<h1>Index</h1>
<div class="toc">
<ul></ul>
</div>
<ul id="index">
<li><h3><a href="#header-classes">Classes</a></h3>
<ul>
<li>
<h4><code><a title="pangaeapy/pandataset.PanAuthor" href="#pangaeapy/pandataset.PanAuthor">PanAuthor</a></code></h4>
<ul class="">
<li><code><a title="pangaeapy/pandataset.PanAuthor.__init__" href="#pangaeapy/pandataset.PanAuthor.__init__">__init__</a></code></li>
</ul>
</li>
<li>
<h4><code><a title="pangaeapy/pandataset.PanDataSet" href="#pangaeapy/pandataset.PanDataSet">PanDataSet</a></code></h4>
<ul class="">
<li><code><a title="pangaeapy/pandataset.PanDataSet.__init__" href="#pangaeapy/pandataset.PanDataSet.__init__">__init__</a></code></li>
<li><code><a title="pangaeapy/pandataset.PanDataSet.setData" href="#pangaeapy/pandataset.PanDataSet.setData">setData</a></code></li>
<li><code><a title="pangaeapy/pandataset.PanDataSet.setID" href="#pangaeapy/pandataset.PanDataSet.setID">setID</a></code></li>
<li><code><a title="pangaeapy/pandataset.PanDataSet.setMetadata" href="#pangaeapy/pandataset.PanDataSet.setMetadata">setMetadata</a></code></li>
</ul>
</li>
<li>
<h4><code><a title="pangaeapy/pandataset.PanEvent" href="#pangaeapy/pandataset.PanEvent">PanEvent</a></code></h4>
<ul class="">
<li><code><a title="pangaeapy/pandataset.PanEvent.__init__" href="#pangaeapy/pandataset.PanEvent.__init__">__init__</a></code></li>
</ul>
</li>
<li>
<h4><code><a title="pangaeapy/pandataset.PanParam" href="#pangaeapy/pandataset.PanParam">PanParam</a></code></h4>
<ul class="">
<li><code><a title="pangaeapy/pandataset.PanParam.__init__" href="#pangaeapy/pandataset.PanParam.__init__">__init__</a></code></li>
<li><code><a title="pangaeapy/pandataset.PanParam.addSynonym" href="#pangaeapy/pandataset.PanParam.addSynonym">addSynonym</a></code></li>
</ul>
</li>
</ul>
</li>
</ul>
</nav>
</main>
