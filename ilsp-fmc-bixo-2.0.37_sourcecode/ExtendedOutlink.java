/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ilsp.fmc.parser;

/* An outgoing link from a page. */
public class ExtendedOutlink {

    private static final String NO_FOLLOW_REL_ATTRIBUTE = "nofollow";

    private String toUrl;
    private String _anchorText;
    private String _relAttributes;
	private String _surroundText;

    public ExtendedOutlink() {
    }

    public ExtendedOutlink(String toUrl, String anchorText,String surroundText, String relAttributes) {
        this.toUrl = toUrl;
        if (anchorText == null)
            anchorText = "";
        _anchorText = anchorText;
        _surroundText = surroundText;
        _relAttributes = relAttributes;
    }
    
    public ExtendedOutlink(String toUrl, String anchorText, String surroundText) {
        this(toUrl, anchorText,surroundText, null);
    }

    public String getToUrl() {
        return toUrl;
    }

    public String getAnchor() {
        return _anchorText;
    }
    public String getSurroundText() {
    	return _surroundText;
    }
    public String getRelAttributes() {
        return _relAttributes;
    }
    
    public boolean isNoFollow() {
        String relAttributesString = getRelAttributes();
        if (relAttributesString != null) {
            String[] relAttributes = relAttributesString.split("[, \t]");
            for (String relAttribute : relAttributes) {
                if (relAttribute.equalsIgnoreCase(NO_FOLLOW_REL_ATTRIBUTE)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    @Override
	public String toString() {
        return "toUrl: " + toUrl + " anchor: " + _anchorText + 
        " surroundText:" + _surroundText; // removed "\n".
        // toString, not
        // printLine... WD.
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((_surroundText == null) ? 0 : _surroundText.hashCode());
		result = prime * result + ((_anchorText == null) ? 0 : _anchorText.hashCode());
		result = prime * result + ((toUrl == null) ? 0 : toUrl.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ExtendedOutlink other = (ExtendedOutlink) obj;
		if (_anchorText == null) {
			if (other._anchorText != null)
				return false;
		} else if (!_anchorText.equals(other._anchorText))
			return false;
		if (toUrl == null) {
			if (other.toUrl != null)
				return false;
		} else if (!toUrl.equals(other.toUrl))
			return false;
		if (_surroundText == null) {
			if (other._surroundText != null)
				return false;
		} else if (!_surroundText.equals(other._surroundText))
			return false;
		return true;
	}
    
    

}
