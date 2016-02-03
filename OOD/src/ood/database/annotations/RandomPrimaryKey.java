/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */


package ood.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation must be declared before a DatabaseRecord field to mean that the correspondent field is a primary key randomly generated. 
 * The type of this field must be an 'int', a 'long', or a {@link java.math.BigInteger}.
 * if the type is an int, the generated value will be a random positive 31 bits value.
 * if the type is a long, the generated value will be a random positive 63 bits value.
 * if the type is a BigInteger, the generated value will be a random positive 128 bits value. 
 * However, you can specify the number of concerned bits by specifying the parameter 'byteNumber'. In all cases, the random value is positive.
 *  
 * @author Jason Mahdjoub
 * @version 1.0
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface RandomPrimaryKey {
    
    /**
     * 
     * @return The number of bits in which the random value is generated.
     */
    int byteNumber() default -1;

}