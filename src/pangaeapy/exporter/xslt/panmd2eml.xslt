<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:md="http://www.pangaea.de/MetaData" xmlns:eml="eml://ecoinformatics.org/eml-2.1.1" xmlns:dc="http://purl.org/dc/terms/">
	<xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
	<xsl:template match="/">
		<eml:eml xmlns:eml="eml://ecoinformatics.org/eml-2.1.1" xmlns:dc="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="eml://ecoinformatics.org/eml-2.1.1 http://rs.gbif.org/schema/eml-gbif-profile/1.1/eml.xsd" system="http://www.pangaea.de" scope="system" xml:lang="eng">
			<xsl:attribute name="packageId">
				<xsl:value-of select="md:MetaData/md:citation/md:URI"/>
			</xsl:attribute>
			<dataset>
				<alternateIdentifier>
					<xsl:value-of select="md:MetaData/md:citation/md:URI"/>
				</alternateIdentifier>
				<alternateIdentifier>
					<xsl:value-of select="md:MetaData/md:citation/@id"/>
				</alternateIdentifier>
				<title xml:lang="eng">
					<xsl:value-of select="md:MetaData/md:citation/md:title"/>
				</title>
				<xsl:for-each select="md:MetaData/md:citation/md:author">
					<creator>
						<individualName>
						<xsl:if test="md:firstName">
							<givenName>
								<xsl:value-of select="md:firstName"/>
							</givenName>
						</xsl:if>
							<surName>
								<xsl:value-of select="md:lastName"/>
							</surName>
						</individualName>
					</creator>
				</xsl:for-each>
				<metadataProvider>
					<organizationName>PANGAEA - Data Publisher for Earth &amp; Environmental Science</organizationName>
					<electronicMailAddress>info@pangaea.de</electronicMailAddress>
					<onlineUrl>https://www.pangaea.de</onlineUrl>
				</metadataProvider>
				<pubDate>
					<xsl:value-of select="md:MetaData/md:citation/md:year"/>
				</pubDate>
				<language>eng</language>
				<abstract><para><xsl:value-of select="md:MetaData/md:abstract"/></para></abstract>
				<intellectualRights>
					<para>
						<xsl:text>This work is licensed under a </xsl:text>
						<ulink>
							<xsl:attribute name="url">
								<xsl:value-of select="md:MetaData/md:license/md:URI"/>
							</xsl:attribute>
							<citetitle>
								<xsl:value-of select="md:MetaData/md:license/md:name"/>
							</citetitle>
						</ulink>.</para>
				</intellectualRights>
				<licensed>
					<licenseName>
						<xsl:value-of select="md:MetaData/md:license/md:name"/>
					</licenseName>
					<url>
						<xsl:value-of select="md:MetaData/md:license/md:URI"/>
					</url>
					<identifier>
						<xsl:value-of select="md:MetaData/md:license/md:label"/>
					</identifier>
				</licensed>
				<distribution>
				<online>
				<url function="information"><xsl:value-of select="md:MetaData/md:citation/md:URI"/></url>
				</online>
				</distribution>
				<coverage>
				<geographicCoverage>
				<geographicDescription>
					<xsl:choose>
						<xsl:when test="md:MetaData/md:event">
							<xsl:for-each select="md:MetaData/md:event">
								<xsl:text>Event: </xsl:text>
								<xsl:value-of select="md:label"/>
								<xsl:text>, </xsl:text>
								<xsl:value-of select="md:location"/>
								<xsl:text>;	</xsl:text>
							</xsl:for-each>
						</xsl:when>
					<xsl:otherwise>
						No textual geographic information given
					</xsl:otherwise>
					</xsl:choose>
				</geographicDescription>
				<boundingCoordinates>
				<westBoundingCoordinate>
				<xsl:value-of select="md:MetaData/md:extent/md:geographic/md:westBoundLongitude"/>
				</westBoundingCoordinate>
				<eastBoundingCoordinate>
				<xsl:value-of select="md:MetaData/md:extent/md:geographic/md:eastBoundLongitude"/>
				</eastBoundingCoordinate>
				<northBoundingCoordinate>
				<xsl:value-of select="md:MetaData/md:extent/md:geographic/md:northBoundLatitude"/>
				</northBoundingCoordinate>
				<southBoundingCoordinate>
				<xsl:value-of select="md:MetaData/md:extent/md:geographic/md:southBoundLatitude"/>
				</southBoundingCoordinate>
				</boundingCoordinates>
				</geographicCoverage>
				</coverage>
				<contact scope="system">
					<individualName>
						<surName>Glöckner</surName>
						<givenName>Frank-Oliver</givenName>
						<role>administrative contact</role>
						<electronicMailAddress>management@pangaea.de</electronicMailAddress>
					</individualName>
				</contact>
				<contact scope="system">
					<individualName>
						<surName>Huber</surName>
						<givenName>Robert</givenName>
						<role>technical contact</role>
						<electronicMailAddress>rhuber@pangaea.de</electronicMailAddress>
					</individualName>
				</contact>
				<xsl:if test="md:MetaData/md:project">
				<project>
				<title>
				<xsl:for-each select="md:MetaData/md:project">
				<xsl:value-of select="md:name"/>; 
				</xsl:for-each>
				</title><personnel><positionName>project member</positionName><role>unknown</role></personnel>
				</project>				
				</xsl:if>
			</dataset>
			<additionalMetadata>
    			<metadata>
      				<gbif>
						<dateStamp><xsl:value-of select="md:MetaData/md:citation/md:dateTime"/></dateStamp>
          				<hierarchyLevel>dataset</hierarchyLevel>
            			<citation>
							<xsl:attribute name="identifier">
								<xsl:value-of select="md:MetaData/md:citation/md:URI"/>
							</xsl:attribute>
						</citation>
					</gbif>
				</metadata>
			</additionalMetadata>
		</eml:eml>
	</xsl:template>
</xsl:stylesheet>
